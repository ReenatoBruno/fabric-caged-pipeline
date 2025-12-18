
# Number of bytes in one mebibyte (MiB).
#   - Used for converting file sizes from bytes to MB
#   - Helps avoid magic numbers in the code
BYTES_PER_MB = 1024 * 1024

# Guardrail to avoid OOM and long runtimes when copying files on the Driver (Fabric)
MAX_FILES_ALLOWED = 500 