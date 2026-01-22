import subprocess
import sys

STEPS = [
    "src/ingest.py",
    "src/clean.py",
    "src/validate.py",
    "src/o2_build_analysis_dataset.py",
    "src/o3_eda.py",
    "src/o3_ml.py",
    "src/o3_federated_sim.py",
    "src/generate_final_report.py"
]

def clean_output(output_str):
    """Filtrează liniile din output."""
    if not output_str:
        return []

    ignore_patterns = [
        "ShutdownHookManager",
        "java.nio.file.NoSuchFileException",
        "winutils",
        "NativeCodeLoader",
        "SparkContext",
        "NativeMethodAccessorImpl",
        "SUCCESS: The process with PID",
        "Setting default log level",
        "WARN SparkConf",
        "WARN MemoryManager",
        "Stage",
        "RowBasedKeyValueBatch",
        "To adjust logging level",
        "For SparkR"
    ]

    clean_lines = []
    for line in output_str.split('\n'):
        if line.strip() and not any(pat in line for pat in ignore_patterns):
            clean_lines.append(line)
    return clean_lines

def run_step(script_path: str):
    print(f"\n Rulare: {script_path} ...", end=" ", flush=True)

    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace'
    )

    if result.returncode == 0:
        print("OK")

        # Procesăm și afișăm doar ce e relevant din STDOUT
        stdout_lines = clean_output(result.stdout)
        if stdout_lines:
            print("\n".join(stdout_lines))

        # Procesăm și afișăm doar erorile reale din STDERR
        stderr_lines = clean_output(result.stderr)
        if stderr_lines:
            print("\n".join(stderr_lines))

    else:
        print("EROARE!")
        print("STDOUT")
        print(result.stdout)
        print("STDERR")
        print(result.stderr)
        sys.exit(1)

def main():
    print("Pornire PROCESARE DATE")

    for script in STEPS:
        run_step(script)

    print("   Rulare finalizata cu SUCCES!")
    print("Verifica raportul in: reports/Raport_Final.txt")

if __name__ == "__main__":
    main()