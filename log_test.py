import logging
logging.basicConfig(level=logging.DEBUG)
logging.warning('Watch out!')  # will print a message to the console
logging.info('I told you so')  # will not print anything
logging.debug('This is a debug message')
logging.info('This is an info message')
logging.warning('This is a warning message')
logging.error('This is an error message')
logging.critical('This is a critical message')