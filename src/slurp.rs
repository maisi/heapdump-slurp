use std::fs::File;
use std::io::{BufRead, BufReader, Read};

use indicatif::{ProgressBar, ProgressStyle};

use crossbeam_channel::{Receiver, Sender};

use crate::errors::HprofSlurpError;
use crate::errors::HprofSlurpError::{
    InvalidHeaderSize, InvalidHprofFile, InvalidIdSize, StdThreadError, UnsupportedDumpFormat,
    UnsupportedIdSize,
};
use crate::java_bridge::analyze_with_java_helper;
use crate::parser::file_header_parser::{FileHeader, parse_file_header};
use crate::parser::record::Record;
use crate::parser::record_stream_parser::HprofRecordStreamParser;
use crate::prefetch_reader::PrefetchReader;
use crate::rendered_result::RenderedResult;
use crate::result_recorder::ResultRecorder;
use crate::utils::pretty_bytes_size;

const FILE_HEADER_LENGTH: usize = 31;

// 64 MB buffer performs nicely (higher is faster but increases the memory consumption)
pub const READ_BUFFER_SIZE: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DumpFormat {
    Hprof,
    Phd,
    OpenJ9Core,
}

fn detect_dump_format(buf: &[u8]) -> Result<DumpFormat, HprofSlurpError> {
    if buf.len() >= 4 && buf.starts_with(&[0x7F, b'E', b'L', b'F']) {
        return Ok(DumpFormat::OpenJ9Core);
    }

    if buf.len() >= 2 {
        let name_len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
        if buf.len() >= 2 + name_len && &buf[2..2 + name_len] == b"portable heap dump" {
            return Ok(DumpFormat::Phd);
        }
    }

    if buf.starts_with(b"JAVA PROFILE") {
        return Ok(DumpFormat::Hprof);
    }

    Err(UnsupportedDumpFormat {
        message: "Unrecognized heap dump signature".to_string(),
    })
}

pub fn slurp_file(
    file_path: String,
    debug_mode: bool,
    list_strings: bool,
) -> Result<RenderedResult, HprofSlurpError> {
    let file = File::open(&file_path)?;
    let file_len = file.metadata()?.len() as usize;
    let mut reader = BufReader::new(file);

    let probe_buffer = reader.fill_buf()?;
    if probe_buffer.is_empty() {
        return Err(UnsupportedDumpFormat {
            message: "Empty input file".to_string(),
        });
    }

    match detect_dump_format(probe_buffer)? {
        DumpFormat::Hprof => slurp_hprof(reader, file_len, debug_mode, list_strings),
        DumpFormat::Phd => {
            drop(reader);
            analyze_with_java_helper("phd", &file_path, file_len as u64, list_strings)
        }
        DumpFormat::OpenJ9Core => {
            drop(reader);
            analyze_with_java_helper("openj9-core", &file_path, file_len as u64, list_strings)
        }
    }
}

fn slurp_hprof(
    mut reader: BufReader<File>,
    file_len: usize,
    debug_mode: bool,
    list_strings: bool,
) -> Result<RenderedResult, HprofSlurpError> {
    let header = slurp_header(&mut reader)?;
    let id_size = header.size_pointers;
    println!(
        "Processing {} binary hprof file in '{}' format.",
        pretty_bytes_size(file_len as u64),
        header.format
    );

    // Communication channel from pre-fetcher to parser
    let (send_data, receive_data): (Sender<Vec<u8>>, Receiver<Vec<u8>>) =
        crossbeam_channel::unbounded();

    // Communication channel from parser to pre-fetcher (pooled input buffers)
    let (send_pooled_data, receive_pooled_data): (Sender<Vec<u8>>, Receiver<Vec<u8>>) =
        crossbeam_channel::unbounded();

    // Init pooled binary data with more than 1 element to enable the reader to make progress interdependently
    for _ in 0..2 {
        send_pooled_data
            .send(Vec::with_capacity(READ_BUFFER_SIZE))
            .expect("pre-fetcher channel should be alive");
    }

    // Communication channel from parser to recorder
    let (send_records, receive_records): (Sender<Vec<Record>>, Receiver<Vec<Record>>) =
        crossbeam_channel::unbounded();

    // Communication channel from recorder to parser (pooled record buffers)
    let (send_pooled_vec, receive_pooled_vec): (Sender<Vec<Record>>, Receiver<Vec<Record>>) =
        crossbeam_channel::unbounded();

    // Communication channel from recorder to main
    let (send_result, receive_result): (Sender<RenderedResult>, Receiver<RenderedResult>) =
        crossbeam_channel::unbounded();

    // Communication channel from parser to main
    let (send_progress, receive_progress): (Sender<usize>, Receiver<usize>) =
        crossbeam_channel::unbounded();

    // Init pre-fetcher
    let prefetcher = PrefetchReader::new(reader, file_len, FILE_HEADER_LENGTH, READ_BUFFER_SIZE);
    let prefetch_thread = prefetcher.start(send_data, receive_pooled_data)?;

    // Init pooled result vec
    send_pooled_vec
        .send(Vec::new())
        .expect("recorder channel should be alive");

    // Init stream parser
    let initial_loop_buffer = Vec::with_capacity(READ_BUFFER_SIZE); // will be added to the data pool after the first chunk
    let stream_parser = HprofRecordStreamParser::new(
        debug_mode,
        file_len,
        FILE_HEADER_LENGTH,
        initial_loop_buffer,
    );

    // Start stream parser
    let parser_thread = stream_parser.start(
        receive_data,
        send_pooled_data,
        send_progress,
        receive_pooled_vec,
        send_records,
    )?;

    // Init result recorder
    let result_recorder = ResultRecorder::new(id_size, list_strings);
    let recorder_thread = result_recorder.start(receive_records, send_result, send_pooled_vec)?;

    // Init progress bar
    let pb = ProgressBar::new(file_len as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} (speed:{bytes_per_sec}) (eta:{eta})")
            .expect("templating should never fail")
            .progress_chars("#>-"),
    );

    // Feed progress bar
    while let Ok(processed) = receive_progress.recv() {
        pb.set_position(processed as u64);
    }

    // Finish and remove progress bar
    pb.finish_and_clear();

    // Wait for final result
    let rendered_result = receive_result
        .recv()
        .expect("result channel should be alive");

    // Blocks until pre-fetcher is done
    prefetch_thread.join().map_err(|e| StdThreadError { e })?;

    // Blocks until parser is done
    parser_thread.join().map_err(|e| StdThreadError { e })?;

    // Blocks until recorder is done
    recorder_thread.join().map_err(|e| StdThreadError { e })?;

    Ok(rendered_result)
}

pub fn slurp_header(reader: &mut BufReader<File>) -> Result<FileHeader, HprofSlurpError> {
    let mut header_buffer = vec![0; FILE_HEADER_LENGTH];
    reader.read_exact(&mut header_buffer)?;
    let (rest, header) = parse_file_header(&header_buffer).map_err(|e| InvalidHprofFile {
        message: format!("{e:?}"),
    })?;
    // Invariants
    let id_size = header.size_pointers;
    if id_size != 4 && id_size != 8 {
        return Err(InvalidIdSize);
    }
    if id_size == 4 {
        return Err(UnsupportedIdSize {
            message: "32 bits heap dumps are not supported yet".to_string(),
        });
    }
    if !rest.is_empty() {
        return Err(InvalidHeaderSize);
    }
    Ok(header)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const FILE_PATH_32: &str = "test-heap-dumps/hprof-32.bin";

    const FILE_PATH_64: &str = "test-heap-dumps/hprof-64.bin";
    const FILE_PATH_RESULT_64: &str = "test-heap-dumps/hprof-64-result.txt";

    fn validate_gold_rendered_result(render_result: RenderedResult, gold_path: &str) {
        let gold = fs::read_to_string(gold_path).expect("gold file not found!");
        // top 20 hardcoded
        let expected = render_result.serialize(20);
        let mut expected_lines = expected.lines();
        for (i1, l1) in gold.lines().enumerate() {
            let l2 = expected_lines.next().unwrap();
            if l1.trim_end() != l2.trim_end() {
                println!("## GOLD line {} ##", i1 + 1);
                println!("{}", l1.trim_end());
                println!("## ACTUAL ##");
                println!("{}", l2.trim_end());
                println!("#####");
                assert_eq!(l1, l2);
            }
        }
    }

    #[test]
    fn detects_hprof_signature() {
        let bytes = b"JAVA PROFILE 1.0.2\0extra";
        assert_eq!(DumpFormat::Hprof, detect_dump_format(bytes).unwrap());
    }

    #[test]
    fn detects_phd_signature() {
        let mut bytes = vec![0x00, 0x12];
        bytes.extend_from_slice(b"portable heap dump");
        bytes.extend_from_slice(&[0, 0, 0, 0]);
        assert_eq!(DumpFormat::Phd, detect_dump_format(&bytes).unwrap());
    }

    #[test]
    fn detects_openj9_core_signature() {
        let bytes = [0x7F, b'E', b'L', b'F', 0x00, 0x00];
        assert_eq!(DumpFormat::OpenJ9Core, detect_dump_format(&bytes).unwrap());
    }

    #[test]
    fn unknown_signature_is_rejected() {
        let bytes = b"?";
        assert!(matches!(
            detect_dump_format(bytes),
            Err(HprofSlurpError::UnsupportedDumpFormat { .. })
        ));
    }

    #[test]
    fn unsupported_32_bits() {
        let file_path = FILE_PATH_32.to_string();
        let result = slurp_file(file_path, false, false);
        assert!(result.is_err());
    }

    #[test]
    fn supported_64_bits() {
        let file_path = FILE_PATH_64.to_string();
        let result = slurp_file(file_path, false, false);
        assert!(result.is_ok());
        validate_gold_rendered_result(result.unwrap(), FILE_PATH_RESULT_64);
    }
}
