import logging

from usigrabber.utils.setup import system_setup

system_setup()

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # Use extra for structured data!
    logger.info("Starting the application with the new formatter.", extra={"project_id": 1234})

    try:
        d = {2: 21}
        d[42]
    except Exception as e:
        # This logs the exception directly including the stack trace.
        # But there is no useful error message
        logger.exception(e)

        # Use this when you want to add a useful message and add a stack trace
        logger.error(f"When accessing ...: {e}", exc_info=True)

        # This adds no traceback!! Only use when you really never want that information
        logger.error(f"When accessing ...: {e}")
