import logging

# Configure logging once here
logging.basicConfig(
    level=logging.INFO
)

logger = logging.getLogger("Master")

class LogEntry:
    def __init__(self, term: int, index: int, data: bytes):
        self.term = term
        self.index = index
        self.data = data
        logger.info(f"Created LogEntry(term={term}, index={index})")

    def display(self):
        logger.info(f"LogEntry: term={self.term}, index={self.index}, data={self.data}")
