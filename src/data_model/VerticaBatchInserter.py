import logging
import os
import queue
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Optional
from datetime import datetime

import vertica_python
from vertica_python.vertica.cursor import Cursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VerticaBatchInserter:
    def __init__(
        self,
        connection_config: dict,
        table: str,
        columns: list[str],
        batch_size: int = 5000,
        max_workers: Optional[int] = None,
        max_connections: int = 10,
    ):
        self.connection_config = connection_config
        self.table = table
        self.columns = columns
        self.batch_size = batch_size
        self.max_workers = max_workers or (os.cpu_count() or 1) * 2
        self.max_connections = max_connections

        self._connection_pool = queue.Queue(maxsize=max_connections)
        self._init_connection_pool()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._lock = threading.Lock()

    def _init_connection_pool(self):
        """Initialize thread-safe connection pool"""
        for _ in range(self.max_connections):
            conn = vertica_python.connect(**self.connection_config)
            self._connection_pool.put(conn)

    def _get_connection(self) -> vertica_python.Connection:
        """Acquire connection from pool with thread safety"""
        return self._connection_pool.get()

    def _return_connection(self, conn: vertica_python.Connection):
        """Return connection to pool"""
        self._connection_pool.put(conn)

    def _format_copy_query(self) -> str:
        """Generate parameterized COPY query"""
        columns = ", ".join(self.columns)
        return f"""
            COPY {self.table} ({columns})
            FROM STDIN 
            DELIMITER ',' 
            ENCLOSED BY '"' 
            NULL '' 
            ESCAPE AS '\\'
            ABORT ON ERROR
        """

    def _batch_generator(self, data: Iterable[tuple]) -> Iterable[list]:
        """Generate batches from input data"""
        batch = []
        for row in data:
            if row is None:  # Sentinel value detected
                if batch:
                    yield batch
                break
            batch.append(row)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _process_batch(self, cursor: Cursor, batch: list[tuple]):
        """Process a single batch with proper error handling"""
        try:
            # Convert batch to CSV format
            csv_data = "\n".join(
                ",".join(
                    str(field).replace('"', '""') if field is not None else ""
                    for field in row
                )
                for row in batch
            )

            # Execute COPY command
            cursor.copy(self._format_copy_query(), csv_data)
        except Exception as e:
            logger.error(f"Batch insert failed: {str(e)}")
            raise

    def insert_data(self, data: Iterable[tuple]):
        """
        Main insertion method with parallel processing
        Returns tuple: (success_count, error_count)
        """
        success = 0
        errors = 0
        self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        futures = []
    
        try:
            for batch in self._batch_generator(data):
                future = self._executor.submit(self._insert_batch, batch)
                futures.append(future)

                # Limit the number of concurrent tasks to the number of connections
                if len(futures) >= self.max_connections:
                    # Wait for at least one task to complete
                    for future in as_completed(futures):
                        try:
                            future.result()
                            success += 1
                        except Exception as e:
                            errors += 1
                            logger.error(f"Batch failed: {traceback.format_exc()}")
                        break
                    futures = futures[1:]  # Remove the completed future

            # Wait for remaining tasks to complete
            for future in as_completed(futures):
                try:
                    future.result()
                    success += 1
                except Exception as e:
                    errors += 1
                    logger.error(f"Batch failed: {traceback.format_exc()}")
    
        finally:
            if self._executor:
                self._executor.shutdown(wait=True)
    
        return success, errors

    def _insert_batch(self, batch: list[tuple]):
        """Thread-safe batch insert operation"""
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                self._process_batch(cursor, batch)
                conn.commit()
        except vertica_python.errors.Error as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn and not conn.closed:
                self._return_connection(conn)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup resources"""
        self.close()

    def close(self):
        """Close all connections in the pool"""
        with self._lock:
            while not self._connection_pool.empty():
                try:
                    conn = self._connection_pool.get_nowait()
                    if not conn.closed:
                        conn.close()
                except queue.Empty:
                    break

# Usage Example
if __name__ == "__main__":
    config = {
        "host": "vertica-ce",
        "port": 5433,
        "user": "cognos",
        "password": "admin1234",
        "database": "vdb",
    }
    
    table = "omniq.users"
    columns = ["user_id", "username", "created_at"]
    
    # Generate sample data (replace with real data)
    def generate_data(max_rows=10000):
        for i in range(max_rows):
            yield (i, f"name_{i}", datetime.now())
        yield None  # Sentinel value to signal end of data
    
    with VerticaBatchInserter(
        connection_config=config,
        table=table,
        columns=columns,
        batch_size=5000,
        max_workers=8,
        max_connections=10
    ) as inserter:
        success, errors = inserter.insert_data(generate_data(max_rows=10000))  # Limited dataset

    logger.info(f"Insert completed: {success} successful batches, {errors} failed batches")