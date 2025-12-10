import logging
import os

def setup_logging(log_file: str = 'project.log'):
    '''
    Configures the logging system for the application, setting the log level 
    and output handlers based on environment variables.
    '''
    # ---Get envirorment variables and map valid log levels ---
    log_level_env = os.getenv('LOG_LEVEL', 'INFO').upper()
    environment = os.getenv('ENVIRONMENT', 'dev').lower()

    valid_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }

    # --- Fallback ---
    log_level = valid_levels.get(log_level_env, logging.INFO)

    # --- Create log dictionary ---
    os.makedirs('logs', exist_ok=True)
    log_path = os.path.join('logs', log_file)

    # --- Basic logging configuration ---
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler() if environment == 'dev' else logging.FileHandler(log_path)
        ]
    )

    logging.info(
        f'Logging initialized with level: {log_level_env} | ENV: {environment}'
    )