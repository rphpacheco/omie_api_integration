# Changelog

## [1.0.0] - 2024-03-03

### Performance Optimizations

#### 1. Pagination System Improvements
- **Concurrent Processing**
  - Added `ThreadPoolExecutor` for parallel page fetching
  - Configurable number of workers (default: 5)
  - Each page fetch runs in a separate thread
  - Improved overall data fetching speed

- **Batch Processing**
  - Implemented batch processing with configurable size (default: 10 pages)
  - Reduced number of database operations
  - Better memory management
  - More efficient data handling

#### 2. Database Optimizations
- **Connection Pooling**
  - Added SQLAlchemy connection pooling with `QueuePool`
  - Configurable pool settings:
    ```python
    pool_size=5
    max_overflow=10
    pool_timeout=30
    pool_pre_ping=True
    ```
  - Better connection management and reuse
  - Improved performance under concurrent loads

- **Transaction Management**
  - Added `execute_with_transaction` method for proper transaction handling
  - Using `with self.engine.begin()` for automatic transaction management
  - Better error handling and rollback support
  - Proper cleanup of resources

- **Data Type Handling**
  - Improved numeric type handling:
    ```python
    numeric_columns = [
        'nSaldo', 'nValorDocumento', 'nSaldoAnterior', 'nSaldoAtual',
        'nSaldoConciliado', 'nSaldoProvisorio', 'nLimiteCreditoTotal',
        'nSaldoDisponivel'
    ]
    ```
  - Using proper SQLAlchemy types (Numeric(15,2) for decimals)
  - Better handling of NULL and empty values
  - Proper type conversion and validation


### Code Structure Improvements

#### 1. Database Class Enhancements
- **New Methods**
  - Added `table_exists` method
  - Added `execute_with_transaction` method
  - Improved `save_into_db` method
  - Better transaction management

### Why These Changes?

1. **Performance**
   - The concurrent processing significantly reduces data fetching time
   - Batch processing reduces database load
   - Connection pooling improves resource utilization
   - Better memory management prevents memory leaks

2. **Reliability**
   - Better transaction management prevents data corruption

### Configuration Examples


#### Pagination Settings
```python
self.batch_size = 10  # Number of pages per batch
self.max_workers = 5  # Number of concurrent workers
```

### Future Improvements
1. Add monitoring and metrics collection
2. Implement caching for frequently accessed data
3. Add more comprehensive error reporting
4. Implement data validation before saving
5. Add support for bulk operations
6. Improve logging and debugging capabilities

### Breaking Changes
- Changed database column types for numeric fields
- Modified transaction handling
- Updated API retry mechanism
- Changed batch processing behavior

### Dependencies
- Added SQLAlchemy connection pooling
- Updated pandas data type handling
- Added concurrent.futures for parallel processing
- Enhanced logging with loguru 