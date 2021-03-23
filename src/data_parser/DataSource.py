import re
import glob
import multiprocessing as mp
import itertools
import pandas as pd


# Function is defined at root level to allow them to be pickleable for multiprocessing.
def regex_groups(regex, line, on_error):
    """Function encapsulating regex search, for use with multiprocessing library. It will return the matching groups in a tuple, and raise errors if on_error is set to 'raise'.
    Explanation why this function is needed:
        Instead of returning re.match objects which are not picklable, re.match.groups which are tuples are returned instead. The return of this function must be picklable for multiprocessing to work, hence this encapsulation is necessary.
        Also if raw logs are huge, we would want the error to be raised as soon as possible. Normal regex searches will only return None if regex fails to match, hence this function also raises a ValueError when on_error is set to 'raise'

    Args:
        regex (re.compiled): Regex to match to line.
        line (str): String to parse.
        on_error (str): 'raise' or 'ignore'. If raise is set, ValueError will be raised when regex fails to match. If ignore is set, None value will be returned as per normal regex behaviour.

    Returns:
        match.groups() or None (tuple/None): If regex matches - match.groups() will be returned, otherwise None is returned.

    Raises:
        ValueError: ValueError is raised if on_error is set to 'raise' and regex fails to match.
    """
    if match := re.search(regex, line):
        return match.groups()
    else:
        if on_error == 'ignore':
            return match  # None will be returned when re.search fails.
        else:  # on_error == 'raise'
            raise ValueError(f'Regex failed to match: {line}')


class DataSource:
    def __init__(self, path, mode='local'):
        """
        Args:
            path (str): Glob-like filename matching of files.

        Opt args:
            mode (str): 'local'(default) or 'spark'. Select depending on the available backend. Local uses pandas dataframes while spark uses spark dataframes.

        Raises:
            ValueError: If an unexpected mode is passed.
        """
        if mode not in ['local', 'spark']:
            raise ValueError(f'Expected "local" or "spark", but received {mode}.')

        self.path = path
        self.mode = mode
        self._load()

    def _load(self):
        """Load the data for processing.
        local - Glob match and lazily load the data.
        spark - Glob match and lazily create the rdd.
        """
        if self.mode == 'local':
            lazy_readers = [open(filename, 'r') for filename in glob.glob(self.path)]
            self.data = itertools.chain(*lazy_readers)  # Concatenate multiple generators
        else:  # spark
            from pyspark.sql import SparkSession
            spark = SparkSession\
                    .builder\
                    .appName('Data_Parsing')\
                    .getOrCreate()
            sc = spark.sparkContext
            self.data = sc.textFile(self.path)

    def parse(self, regex, columns, on_error='raise'):
        """Parse the raw logs into a dataframe.
        Args:
            regex (str): Regex to match raw logs. Must contain capturing groups equal to number of columns.
            columns (list of str): List of column names.

        Opt Args:
            on_error (str): 'raise'(default)/'ignore'. If the regex doesn't match a line, raise will raise an error. ignore will ignore it and remove the line.

        Returns:
            Dataframe: Local mode returns pandas dataframe. Spark mode returns spark dataframe.

        Raises:
            ValueError: If regex doesn't contain equal number of capturing groups as there are columns.
            ValueError: If something other than 'raise' or 'ignore' is passed into on_error.
        """
        regex = re.compile(regex)
        if regex.groups != len(columns):
            raise ValueError('Number of regex groups and number of columns passed do not match.')

        if on_error not in ['raise', 'ignore']:
            raise ValueError(f'Expected "raise"/"ignore" but got {on_error}.')

        if self.mode == 'local':
            # Create an argument generator for starmap
            args = ((regex, line, on_error) for line in self.data)
            with mp.Pool() as p:
                parsed_data = p.starmap(regex_groups, args)  # Apply custom regex search function, passing in each arg.
            parsed_data = filter(bool, parsed_data)
            df = pd.DataFrame.from_records(parsed_data, columns=columns)

        elif self.mode == 'spark':
            df = self.data.map(lambda line: regex_groups(regex, line, on_error)).filter(bool).toDF(columns)
        return df
