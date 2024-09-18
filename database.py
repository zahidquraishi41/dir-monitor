import os
import sys
import sqlite3 as sql
from time import time_ns
from typing import List, Tuple
from datetime import datetime as dt


class Database:
    def __init__(self) -> None:
        self.db_path = os.path.join(
            os.path.dirname(sys.argv[0]),
            'records.db'
        )
        self.con = sql.Connection(self.db_path)
        self.cur = self.con.cursor()
        self.__create_tables()

    def record(self, path: str) -> str:
        """Records the files in the specified directory and stores the metadata in a new table.

        Args:
            path (str): The path of the directory to record.

        Returns:
            str: The name of the table where the record is stored.
        """
        table_name = self.__create_new_table()
        files = []
        for root, _, filenames in os.walk(path):
            for filename in filenames:
                stats = os.stat(os.path.join(root, filename))
                files.append([
                    str(stats.st_ino),
                    root, filename,
                    str(stats.st_mtime_ns)
                ])

        self.cur.executemany(
            f'INSERT INTO {table_name} VALUES (?,?,?,?)',
            files
        )
        self.cur.execute(
            '''INSERT INTO tables VALUES (?,?,?)''',
            (table_name, path, dt.now().strftime('%d/%m/%Y %H:%M:%S'))
        )
        self.con.commit()

        return table_name

    def erase(self, path: str, date: str) -> None:
        '''Erases the record for a specified directory and date.

        Args:
            path (str): The path of the directory.
            date (str): The date when the directory was recorded.
        '''

        table_name = self.cur.execute(
            '''SELECT table_name FROM tables WHERE path=? AND date=?''',
            (path, date)
        ).fetchone()
        table_name = table_name[0]
        self.cur.execute(
            f'DELETE FROM tables WHERE table_name=?', (table_name, ))
        self.cur.execute(f'DROP TABLE {table_name}')
        self.con.commit()

    def recorded(self) -> List[Tuple[str, str]]:
        '''Returns a list of recorded directories along with date and time.'''
        statement = 'SELECT path, date FROM tables'
        return self.cur.execute(statement).fetchall()

    def changes(self, path: str, date: str) -> dict:
        '''Returns the changes made to a recorded path.

        Args:
            path (str): The path of the directory.
            date (str): The date when the directory was recorded.

        Returns:
            dict: A dictionary containing lists of added, modified, moved, deleted, and renamed files.
        '''
        old_table = self.cur.execute(
            'SELECT table_name FROM tables WHERE path=? AND date=?',
            (path, date)
        ).fetchone()[0]
        new_table = self.record(path)
        output = {
            'added': [], 'deleted': [], 'moved': [],
            'renamed': [], 'modified': []
        }

        # Added
        query = f'''SELECT t1.dir, t1.filename
        FROM {new_table} t1
        LEFT JOIN {old_table} t2 ON t1.inode = t2.inode
        WHERE t2.inode IS NULL'''
        added = self.cur.execute(query).fetchall()
        for dir, filename in added:
            output['added'].append(
                os.path.join(dir, filename)
            )

        # Deleted
        query = f'''SELECT t1.dir, t1.filename
        FROM {old_table} t1
        LEFT JOIN {new_table} t2 ON t1.inode = t2.inode
        WHERE t2.inode IS NULL'''
        deleted = self.cur.execute(query).fetchall()
        for dir, filename in deleted:
            output['deleted'].append(
                os.path.join(dir, filename)
            )

        # Modified
        query = f'''SELECT old.dir, old.filename
            FROM {old_table} AS old
            JOIN {new_table} AS new
            ON old.inode = new.inode
            WHERE old.mtime <> new.mtime'''
        modified = self.cur.execute(query).fetchall()
        for dir, filename in modified:
            output['modified'].append(
                os.path.join(dir, filename)
            )

        # Moved
        query = f'''SELECT old.dir, old.filename, new.dir
            FROM {old_table} AS old
            JOIN {new_table} AS new
            ON old.inode = new.inode
            WHERE old.dir <> new.dir'''
        moved = self.cur.execute(query).fetchall()
        for dir, filename, new_dir in moved:
            output['moved'].append(
                os.path.join(dir, filename) + ' => ' + new_dir
            )

        # Renamed
        query = f'''SELECT old.dir, old.filename, new.filename
            FROM {old_table} AS old
            JOIN {new_table} AS new ON old.inode = new.inode
            WHERE old.filename <> new.filename'''
        renamed = self.cur.execute(query).fetchall()
        for dir, filename, new_filename in renamed:
            output['renamed'].append(
                os.path.join(dir, filename) + ' => ' + new_filename
            )

        # Cleaning
        self.cur.execute(
            'DELETE FROM tables WHERE table_name=?',
            (new_table, )
        )
        self.cur.execute(f'DROP TABLE {new_table}')
        self.con.commit()

        return output

    def __create_new_table(self) -> str:
        '''Creates a table for storing files metadata.'''
        table_name = 't' + str(time_ns())
        statement = f'''CREATE TABLE {table_name} (
            inode TEXT PRIMARY KEY,
            dir TEXT,
            filename TEXT,
            mtime TEXT
        )'''
        self.cur.execute(statement)
        return table_name

    def __create_tables(self):
        '''Creates table for tracking recorded directories.'''
        statement = '''CREATE TABLE IF NOT EXISTS tables (
            table_name TEXT PRIMARY KEY,
            path TEXT,
            date TEXT
        )'''
        self.cur.execute(statement)

    def close(self):
        '''Closes the connection to the SQLite database.'''
        self.con.close()
