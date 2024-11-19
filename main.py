import logging
import importlib
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def dynamic_import(db_type, extraction):
    logger.info(f"Attempting to import module for {db_type} - {extraction}")
    try:
        module_name = f"engines.{db_type}.{db_type}_{extraction}"
        module = importlib.import_module(module_name)
        logger.info(f"Successfully imported module: {module_name}")
        return module
    except ImportError as e:
        logger.error(f"Error importing module {db_type}_{extraction}: {str(e)}")
        return None

def extractor(db_type, extraction):
    logger.info(f"Starting extractor for engine: {db_type}, extraction type: {extraction}")
    module = dynamic_import(db_type, extraction)
    if module:
        if extraction == 'metadata':
            logger.info(f"Running {db_type.capitalize()} Metadata extractor...")
            try:
                module.extract_metadata()
            except Exception as e:
                logger.error(f"Error during metadata extraction: {str(e)}")
        elif extraction == 'querylogs':
            logger.info(f"Running {db_type.capitalize()} Query Log extractor...")
            try:
                module.extract_query_logs()
            except Exception as e:
                logger.error(f"Error during query log extraction: {str(e)}")
    else:
        logger.error(f"Module not found or failed to load for {db_type}_{extraction}")


if __name__ == "__main__":
    logger.info("Parsing command-line arguments.")
    parser = argparse.ArgumentParser()
    parser.add_argument('db_type', type=str)
    parser.add_argument('extraction', type=str)

    args = parser.parse_args()
    logger.info(f"Starting extractor script with db_type: {args.db_type}, extraction: {args.extraction}")

    extractor(args.db_type, args.extraction)
    logger.info("Extractor script completed.")



