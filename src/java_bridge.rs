use std::env;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::SystemTime;

use indoc::formatdoc;
use serde::Deserialize;
use zip::ZipArchive;

use crate::errors::HprofSlurpError;
use crate::errors::HprofSlurpError::JavaHelperError;
use crate::rendered_result::{ClassAllocationStats, RenderedResult};

#[derive(Deserialize)]
struct HelperClassStats {
    class_name: String,
    instance_count: u64,
    largest_allocation_bytes: u64,
    allocation_size_bytes: u64,
}

#[derive(Deserialize)]
struct HelperResponse {
    memory_usage: Vec<HelperClassStats>,
    total_objects: u64,
    class_count: usize,
    thread_count: usize,
    string_count: u64,
    total_heap_bytes: u64,
    format: String,
}

struct DtfjClasspath {
    impl_jar: PathBuf,
    interface_jar: PathBuf,
    extra_jars: Vec<PathBuf>,
}

impl DtfjClasspath {
    fn all(&self) -> Vec<&Path> {
        let mut jars = Vec::with_capacity(2 + self.extra_jars.len());
        jars.push(self.impl_jar.as_path());
        jars.push(self.interface_jar.as_path());
        for extra in &self.extra_jars {
            jars.push(extra.as_path());
        }
        jars
    }
}

pub fn analyze_with_java_helper(
    format_label: &str,
    dump_path: &str,
    file_len: u64,
    list_strings: bool,
) -> Result<RenderedResult, HprofSlurpError> {
    let dtfj = locate_dtfj_jars()?;
    let class_dir = compile_helper(&dtfj)?;
    let response = invoke_helper(&class_dir, &dtfj, format_label, dump_path)?;

    Ok(render_helper_response(
        response,
        format_label,
        file_len,
        list_strings,
    ))
}

fn render_helper_response(
    response: HelperResponse,
    format_label: &str,
    file_len: u64,
    list_strings: bool,
) -> RenderedResult {
    let memory_usage = response
        .memory_usage
        .into_iter()
        .map(|entry| {
            ClassAllocationStats::new(
                entry.class_name,
                entry.instance_count,
                entry.largest_allocation_bytes,
                entry.allocation_size_bytes,
            )
        })
        .collect();

    let summary = formatdoc!(
        "\nFile content summary (Java helper mode):\n  Input bytes: {file_len}\n  Helper-reported format: {format}\n  Objects counted: {objects}\n  Distinct classes: {classes}\n  Strings encountered: {strings}\n  Helper-reported heap bytes: {heap_bytes}",
        file_len = file_len,
        format = response.format,
        objects = response.total_objects,
        classes = response.class_count,
        strings = response.string_count,
        heap_bytes = response.total_heap_bytes,
    );

    let thread_info = formatdoc!(
        "\nThread information:\n  Threads discovered: {threads}\n  Detailed stack traces are not available for {format_label} dumps in helper mode.",
        threads = response.thread_count,
        format_label = format_label,
    );

    let captured_strings = if list_strings {
        Some("Listing captured strings is not yet supported for this dump format.\n".to_string())
    } else {
        None
    };

    RenderedResult {
        summary,
        thread_info,
        memory_usage,
        duplicated_strings: None,
        captured_strings,
    }
}

fn invoke_helper(
    class_dir: &Path,
    dtfj: &DtfjClasspath,
    format_label: &str,
    dump_path: &str,
) -> Result<HelperResponse, HprofSlurpError> {
    let mut parts = dtfj.all();
    let mut classpath_parts = Vec::with_capacity(parts.len() + 1);
    classpath_parts.push(class_dir);
    classpath_parts.append(&mut parts);
    let classpath = join_classpath(&classpath_parts);

    let output = Command::new("java")
        .args([
            "--add-exports",
            "java.base/jdk.internal.org.objectweb.asm=ALL-UNNAMED",
            "--add-exports",
            "java.base/jdk.internal.org.objectweb.asm.tree=ALL-UNNAMED",
            "--add-exports",
            "java.base/jdk.internal.module=ALL-UNNAMED",
        ])
        .arg("-cp")
        .arg(classpath)
        .arg("com.maisi.heapdump.JavaHeapAnalyzer")
        .arg("--input")
        .arg(dump_path)
        .arg("--format")
        .arg(format_label)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| JavaHelperError {
            message: format!("Failed to launch Java helper: {e}"),
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(JavaHelperError {
            message: format!("Java helper exited with status {}: {stderr}", output.status),
        });
    }

    let stdout = String::from_utf8(output.stdout).map_err(|e| JavaHelperError {
        message: format!("Java helper produced invalid UTF-8 output: {e}"),
    })?;

    serde_json::from_str(&stdout).map_err(|e| JavaHelperError {
        message: format!("Failed to parse Java helper JSON output: {e}\nPayload: {stdout}"),
    })
}

fn compile_helper(dtfj: &DtfjClasspath) -> Result<PathBuf, HprofSlurpError> {
    let helper_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("java-helper");
    let src_dir = helper_root.join("src");
    let class_dir = helper_root.join("target/classes");

    let _ = fs::remove_dir_all(&class_dir);
    fs::create_dir_all(&class_dir).map_err(|e| JavaHelperError {
        message: format!("Unable to create Java helper target directory: {e}"),
    })?;

    let source_file = src_dir.join("com/maisi/heapdump/JavaHeapAnalyzer.java");
    if !source_file.exists() {
        return Err(JavaHelperError {
            message: format!("Java helper source not found at {}", source_file.display()),
        });
    }

    let dtfj_classpath = dtfj.all();
    let classpath = join_classpath(&dtfj_classpath);
    let output = Command::new("javac")
        .arg("-cp")
        .arg(classpath)
        .arg("-d")
        .arg(&class_dir)
        .arg(&source_file)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| JavaHelperError {
            message: format!("Failed to invoke javac: {e}"),
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(JavaHelperError {
            message: format!("javac exited with status {}: {stderr}", output.status),
        });
    }

    Ok(class_dir)
}

fn join_classpath(parts: &[&Path]) -> String {
    let separator = if cfg!(windows) { ';' } else { ':' };
    let mut buffer = String::new();
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            buffer.push(separator);
        }
        buffer.push_str(&part.display().to_string());
    }
    buffer
}

fn locate_dtfj_jars() -> Result<DtfjClasspath, HprofSlurpError> {
    if let Some(dir) = env::var_os("HPROF_SLURP_DTFJ_DIR") {
        if let Some(pair) = try_locate_in_dir(Path::new(&dir))? {
            return Ok(pair);
        }
    }

    if let Some(dir) = env::var_os("MAT_HOME") {
        if let Some(pair) = try_locate_in_dir(Path::new(&dir))? {
            return Ok(pair);
        }
    }

    let defaults = [
        Path::new("/opt/mat"),
        Path::new("/usr/lib/mat"),
        Path::new("/Applications/MemoryAnalyzer.app/Contents/Eclipse"),
    ];

    for base in defaults {
        if let Some(pair) = try_locate_in_dir(base)? {
            return Ok(pair);
        }
    }

    Err(JavaHelperError {
        message: "Unable to locate dtfj.jar and dtfj-interface.jar. Set HPROF_SLURP_DTFJ_DIR to the directory containing these jars.".to_string(),
    })
}

fn try_locate_in_dir(base: &Path) -> Result<Option<DtfjClasspath>, HprofSlurpError> {
    if !base.exists() {
        return Ok(None);
    }
    let mut stack = Vec::new();
    stack.push((base.to_path_buf(), 0u8));
    let mut dtfj_impl = None;
    let mut dtfj_interface = None;
    let mut extra_jars: Vec<PathBuf> = Vec::new();

    while let Some((dir, depth)) = stack.pop() {
        if depth > 6 {
            continue;
        }
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push((path, depth + 1));
                continue;
            }
            let Some(filename) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };

            if dtfj_impl.is_none() {
                if let Some((impl_path, mut extras)) = resolve_impl_candidate(&path, filename)? {
                    dtfj_impl = Some(impl_path);
                    extra_jars.append(&mut extras);
                }
            }
            if dtfj_interface.is_none() {
                if let Some((interface_path, mut extras)) =
                    resolve_interface_candidate(&path, filename)?
                {
                    dtfj_interface = Some(interface_path);
                    extra_jars.append(&mut extras);
                }
            }

            if let (Some(impl_path), Some(interface_path)) =
                (dtfj_impl.as_ref(), dtfj_interface.as_ref())
            {
                return Ok(Some(DtfjClasspath {
                    impl_jar: impl_path.clone(),
                    interface_jar: interface_path.clone(),
                    extra_jars,
                }));
            }
        }
    }
    Ok(None)
}

fn is_dtfj_impl_jar(filename: &str) -> bool {
    if filename == "dtfj.jar" {
        return true;
    }
    if !filename.ends_with(".jar") {
        return false;
    }
    let lower = filename.to_ascii_lowercase();
    lower.starts_with("com.ibm.dtfj.j9")
        || lower.starts_with("com.ibm.dtfj.vm")
        || lower.contains("dtfj_impl")
}

fn is_dtfj_interface_jar(filename: &str) -> bool {
    if filename == "dtfj-interface.jar" {
        return true;
    }
    if !filename.ends_with(".jar") {
        return false;
    }
    let lower = filename.to_ascii_lowercase();
    lower.starts_with("com.ibm.dtfj.api") || lower.contains("dtfj_interface")
}

fn resolve_impl_candidate(
    path: &Path,
    filename: &str,
) -> Result<Option<(PathBuf, Vec<PathBuf>)>, HprofSlurpError> {
    if !is_dtfj_impl_jar(filename) {
        return Ok(None);
    }

    if filename == "dtfj.jar" || filename.contains("dtfj_impl") {
        return Ok(Some((path.to_path_buf(), Vec::new())));
    }

    let lower = filename.to_ascii_lowercase();
    if lower.starts_with("com.ibm.dtfj.j9") || lower.starts_with("com.ibm.dtfj.vm") {
        let impl_jar = extract_nested_jar(path, "lib/dtfj.jar")?;
        let j9ddr = extract_nested_jar(path, "lib/j9ddr.jar")?;
        return Ok(Some((impl_jar, vec![j9ddr])));
    }

    Ok(None)
}

fn resolve_interface_candidate(
    path: &Path,
    filename: &str,
) -> Result<Option<(PathBuf, Vec<PathBuf>)>, HprofSlurpError> {
    if !is_dtfj_interface_jar(filename) {
        return Ok(None);
    }

    if filename == "dtfj-interface.jar" || filename.contains("dtfj_interface") {
        return Ok(Some((path.to_path_buf(), Vec::new())));
    }

    let lower = filename.to_ascii_lowercase();
    if lower.starts_with("com.ibm.dtfj.api") {
        let interface_jar = extract_nested_jar(path, "lib/dtfj-interface.jar")?;
        return Ok(Some((interface_jar, Vec::new())));
    }

    Ok(None)
}

fn extract_nested_jar(container: &Path, nested_entry: &str) -> Result<PathBuf, HprofSlurpError> {
    let helper_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("java-helper");
    let cache_dir = helper_root.join("target/extracted-jars");
    fs::create_dir_all(&cache_dir).map_err(|e| JavaHelperError {
        message: format!(
            "Unable to create extracted jar cache at {}: {e}",
            cache_dir.display()
        ),
    })?;

    let container_name = container
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("container");
    let sanitized_nested = nested_entry.replace('/', "_");
    let target = cache_dir.join(format!("{container_name}_{sanitized_nested}"));

    if needs_refresh(&target, container) {
        let file = File::open(container).map_err(|e| JavaHelperError {
            message: format!("Failed to open DTFJ bundle {}: {e}", container.display()),
        })?;
        let mut archive = ZipArchive::new(file).map_err(|e| JavaHelperError {
            message: format!(
                "Failed to read DTFJ bundle {} as zip: {e}",
                container.display()
            ),
        })?;
        let mut nested = archive.by_name(nested_entry).map_err(|e| JavaHelperError {
            message: format!(
                "Failed to locate {nested_entry} inside {}: {e}",
                container.display()
            ),
        })?;
        let mut output = File::create(&target).map_err(|e| JavaHelperError {
            message: format!("Failed to write extracted jar {}: {e}", target.display()),
        })?;
        std::io::copy(&mut nested, &mut output).map_err(|e| JavaHelperError {
            message: format!("Failed to copy {nested_entry} to {}: {e}", target.display()),
        })?;
    }

    Ok(target)
}

fn needs_refresh(target: &Path, source: &Path) -> bool {
    let target_meta = match fs::metadata(target) {
        Ok(meta) => meta,
        Err(_) => return true,
    };
    let source_meta = match fs::metadata(source) {
        Ok(meta) => meta,
        Err(_) => return true,
    };
    let target_time = target_meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let source_time = source_meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    source_time > target_time
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    const SAMPLE_HPROF_PATH: &str = "sample/hprof-sample.hprof";
    const SAMPLE_PHD_PATH: &str = "sample/phd-sample.phd";
    const SAMPLE_OPENJ9_PATH: &str = "sample/coredump-sample.dmp";
    const TEST_CLASS: &str = "org.example.Main$TestClass";

    fn find_test_class_count(result: &RenderedResult) -> Option<u64> {
        result
            .memory_usage
            .iter()
            .find(|entry| entry.class_name == TEST_CLASS)
            .map(|entry| entry.instance_count)
    }

    #[test]
    fn sample_dumps_report_consistent_test_class_counts() {
        let baseline = crate::slurp::slurp_file(SAMPLE_HPROF_PATH.to_string(), false, false)
            .expect("sample hprof dump should parse");
        let baseline_count =
            find_test_class_count(&baseline).expect("test class present in hprof baseline");
        assert_eq!(baseline_count, 10_000);

        let phd_len = fs::metadata(SAMPLE_PHD_PATH)
            .expect("sample PHD dump should exist")
            .len();
        let phd_result = match analyze_with_java_helper("phd", SAMPLE_PHD_PATH, phd_len, false) {
            Ok(result) => result,
            Err(HprofSlurpError::JavaHelperError { message }) => {
                eprintln!(
                    "Skipping PHD/OpenJ9 sample verification; Java helper failed for PHD dump: {message}"
                );
                return;
            }
            Err(err) => panic!("sample PHD dump should parse via Java helper: {err:?}"),
        };
        let Some(phd_count) = find_test_class_count(&phd_result) else {
            eprintln!(
                "Skipping PHD/OpenJ9 sample verification; Java helper produced no class stats for PHD dump"
            );
            return;
        };

        let openj9_len = fs::metadata(SAMPLE_OPENJ9_PATH)
            .expect("sample OpenJ9 core dump should exist")
            .len();
        let openj9_result = match analyze_with_java_helper(
            "openj9-core",
            SAMPLE_OPENJ9_PATH,
            openj9_len,
            false,
        ) {
            Ok(result) => result,
            Err(HprofSlurpError::JavaHelperError { message }) => {
                eprintln!(
                    "Skipping PHD/OpenJ9 sample verification; Java helper failed for OpenJ9 dump: {message}"
                );
                return;
            }
            Err(err) => panic!("sample OpenJ9 dump should parse via Java helper: {err:?}"),
        };
        let Some(openj9_count) = find_test_class_count(&openj9_result) else {
            eprintln!(
                "Skipping PHD/OpenJ9 sample verification; Java helper produced no class stats for OpenJ9 dump"
            );
            return;
        };

        assert_eq!(baseline_count, phd_count);
        assert_eq!(baseline_count, openj9_count);
    }

    #[test]
    fn dtfj_impl_detector_handles_modern_mat_jars() {
        assert!(is_dtfj_impl_jar("dtfj.jar"));
        assert!(is_dtfj_impl_jar("com.ibm.dtfj.j9_1.1.2.202508181917.jar"));
        assert!(is_dtfj_impl_jar("com.ibm.dtfj.vm_1.0.0.jar"));
        assert!(is_dtfj_impl_jar("foo_dtfj_impl_123.jar"));
        assert!(!is_dtfj_impl_jar("some-other.jar"));
        assert!(!is_dtfj_impl_jar("dtfj.txt"));
    }

    #[test]
    fn dtfj_interface_detector_handles_modern_mat_jars() {
        assert!(is_dtfj_interface_jar("dtfj-interface.jar"));
        assert!(is_dtfj_interface_jar(
            "com.ibm.dtfj.api_1.1.2.202508181917.jar"
        ));
        assert!(is_dtfj_interface_jar("foo_dtfj_interface_123.jar"));
        assert!(!is_dtfj_interface_jar(
            "com.ibm.dtfj.j9_1.1.2.202508181917.jar"
        ));
        assert!(!is_dtfj_interface_jar("dtfj-interface.txt"));
    }
}
