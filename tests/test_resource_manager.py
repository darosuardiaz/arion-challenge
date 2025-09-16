import os
import sys
import pytest
import tempfile
import asyncio
import sqlite3
import logging
from unittest.mock import Mock, patch

# Ensure repository root is on path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from resource_manager import ResourceManager, DatabaseManager, QueueManager


def test_database_manager_context():
    """Test DatabaseManager context manager functionality."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        # Test successful connection
        with DatabaseManager(db_path) as db:
            assert db is not None
            assert isinstance(db, sqlite3.Connection)
            
            # Test schema creation
            tables = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            table_names = [t[0] for t in tables]
            assert 'aggregates' in table_names
            
            # Test basic operations
            db.execute("INSERT INTO aggregates VALUES (?, ?, ?, ?, ?, ?)", 
                      (1640995200, 'test', 100.0, 5, 2, 50.0))
            db.commit()
            
            result = db.execute("SELECT * FROM aggregates").fetchone()
            assert result['category'] == 'test'
            assert result['total_value'] == 100.0
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass


def test_database_manager_no_schema():
    """Test DatabaseManager without schema setup."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        with DatabaseManager(db_path, setup_schema=False) as db:
            # Should not have aggregates table
            tables = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            table_names = [t[0] for t in tables]
            assert 'aggregates' not in table_names
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass


@pytest.mark.asyncio
async def test_queue_manager_context():
    """Test QueueManager context manager functionality."""
    with QueueManager(queue_maxsize=10) as queues:
        assert isinstance(queues, dict)
        assert 'ingest' in queues
        assert 'db' in queues
        assert 'outbound' in queues
        assert 'db_failures' in queues
        assert 'dead_letter' in queues
        
        # Test queue operations
        await queues['ingest'].put({'test': 'data'})
        item = await queues['ingest'].get()
        assert item == {'test': 'data'}


@pytest.mark.asyncio
async def test_queue_manager_custom_names():
    """Test QueueManager with custom queue names."""
    custom_names = ['custom1', 'custom2']
    with QueueManager(queue_maxsize=5, queue_names=custom_names) as queues:
        assert len(queues) == 2
        assert 'custom1' in queues
        assert 'custom2' in queues
        assert 'ingest' not in queues


def test_resource_manager_basic_lifecycle():
    """Test ResourceManager basic lifecycle."""
    def mock_db_factory():
        return Mock()
    
    def mock_mq_factory():
        return Mock()
    
    rm = ResourceManager(db_factory=mock_db_factory, mq_factory=mock_mq_factory)
    
    # Should not be active initially
    assert not rm._active
    
    with rm:
        assert rm._active
        
        # Should be able to get resources
        db = rm.get_database()
        mq = rm.get_message_queue()
        
        assert db is not None
        assert mq is not None
        
        # Should track metrics
        metrics = rm.metrics_snapshot()
        assert metrics['active'] is True
        assert 'acquire_ms' in metrics
        assert 'db' in metrics['acquire_ms']
        assert 'mq' in metrics['acquire_ms']
    
    # Should be inactive after context exit
    assert not rm._active


def test_resource_manager_early_release():
    """Test early release of resources."""
    def mock_db_factory():
        mock_db = Mock()
        mock_db.__enter__ = Mock(return_value=mock_db)
        mock_db.__exit__ = Mock(return_value=False)
        return mock_db
    
    def mock_mq_factory():
        mock_mq = Mock()
        mock_mq.__enter__ = Mock(return_value=mock_mq)
        mock_mq.__exit__ = Mock(return_value=False)
        return mock_mq
    
    rm = ResourceManager(db_factory=mock_db_factory, mq_factory=mock_mq_factory)
    
    with rm:
        # Get resources
        db = rm.get_database()
        mq = rm.get_message_queue()
        
        # Release database early
        assert rm.release_database() is True
        assert rm.release_database() is False  # Second release should be no-op
        
        # Release message queue early
        assert rm.release_message_queue() is True
        assert rm.release_message_queue() is False  # Second release should be no-op
        
        # Should be able to re-acquire
        db2 = rm.get_database()
        mq2 = rm.get_message_queue()
        
        assert db2 is not None
        assert mq2 is not None


def test_resource_manager_attach_context():
    """Test attaching additional context managers."""
    def mock_factory():
        return Mock()
    
    rm = ResourceManager(db_factory=mock_factory, mq_factory=mock_factory)
    
    with rm:
        # Create a mock context manager
        mock_cm = Mock()
        mock_cm.__enter__ = Mock(return_value="context_value")
        mock_cm.__exit__ = Mock(return_value=False)
        
        # Attach context
        result = rm.attach_context(mock_cm, name="test_context", description="test")
        assert result == "context_value"
        
        # Should be in metrics
        metrics = rm.metrics_snapshot()
        assert "test_context" in metrics['attached']
        assert 'acquire_ms' in metrics['attached']['test_context']
        
        # Test early release of attached context
        assert rm.release_context("test_context") is True
        assert rm.release_context("test_context") is False  # Second release should be no-op
        
        # Should have release metrics
        metrics = rm.metrics_snapshot()
        assert metrics['attached']['test_context']['release_ms'] is not None


def test_resource_manager_error_handling():
    """Test ResourceManager error handling."""
    def failing_db_factory():
        raise RuntimeError("Database connection failed")
    
    def mock_mq_factory():
        return Mock()
    
    rm = ResourceManager(db_factory=failing_db_factory, mq_factory=mock_mq_factory)
    
    with rm:
        # Should propagate the exception
        with pytest.raises(RuntimeError, match="Database connection failed"):
            rm.get_database()


def test_resource_manager_use_outside_context():
    """Test ResourceManager raises error when used outside context."""
    def mock_factory():
        return Mock()
    
    rm = ResourceManager(db_factory=mock_factory, mq_factory=mock_factory)
    
    # Should raise error when not in context
    with pytest.raises(RuntimeError, match="Use ResourceManager inside a 'with' block"):
        rm.get_database()
    
    with pytest.raises(RuntimeError, match="Use ResourceManager inside a 'with' block"):
        rm.get_message_queue()
    
    with pytest.raises(RuntimeError, match="Use ResourceManager inside a 'with' block"):
        rm.attach_context(Mock())


def test_resource_manager_factory_for_pipeline():
    """Test ResourceManager.for_pipeline factory method."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        rm = ResourceManager.for_pipeline(
            db_path=db_path,
            queue_maxsize=50,
            setup_schema=True,
            logger=logging.getLogger("test")
        )
        
        with rm:
            # Test database
            db = rm.get_database()
            assert isinstance(db, sqlite3.Connection)
            
            # Test queues
            queues = rm.get_message_queue()
            assert isinstance(queues, dict)
            expected_queues = ["ingest", "db", "outbound", "db_failures", "dead_letter"]
            for queue_name in expected_queues:
                assert queue_name in queues
                assert isinstance(queues[queue_name], asyncio.Queue)
                assert queues[queue_name].maxsize == 50
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass


def test_resource_manager_non_context_resource():
    """Test ResourceManager with non-context manager resources."""
    class MockResource:
        def __init__(self):
            self.closed = False
        
        def close(self):
            self.closed = True
    
    def resource_factory():
        return MockResource()
    
    def mock_mq_factory():
        return Mock()
    
    rm = ResourceManager(db_factory=resource_factory, mq_factory=mock_mq_factory)
    
    with rm:
        resource = rm.get_database()
        assert isinstance(resource, MockResource)
        assert not resource.closed
    
    # Resource should be closed after context exit
    assert resource.closed


def test_resource_manager_invalid_resource():
    """Test ResourceManager with invalid resource type."""
    def invalid_factory():
        return "not a resource"  # String without close/shutdown/disconnect methods
    
    def mock_mq_factory():
        return Mock()
    
    rm = ResourceManager(db_factory=invalid_factory, mq_factory=mock_mq_factory)
    
    with rm:
        with pytest.raises(TypeError, match="non-context object without a close"):
            rm.get_database()


@pytest.mark.asyncio
async def test_resource_manager_metrics_timing():
    """Test ResourceManager metrics capture timing information."""
    def slow_db_factory():
        import time
        time.sleep(0.01)  # Simulate slow initialization
        mock_db = Mock()
        mock_db.__enter__ = Mock(return_value=mock_db)
        mock_db.__exit__ = Mock(return_value=False)
        return mock_db
    
    def fast_mq_factory():
        return Mock()
    
    rm = ResourceManager(db_factory=slow_db_factory, mq_factory=fast_mq_factory)
    
    with rm:
        # Get resources to trigger timing
        rm.get_database()
        rm.get_message_queue()
        
        metrics = rm.metrics_snapshot()
        
        # Should have timing information
        assert metrics['acquire_ms']['db'] is not None
        assert metrics['acquire_ms']['mq'] is not None
        assert metrics['acquire_ms']['db'] > 0
        assert metrics['context_ms'] is None  # Not available until exit
    
    # After context exit, should have context timing
    final_metrics = rm.metrics_snapshot()
    assert final_metrics['context_ms'] is not None
    assert final_metrics['context_ms'] > 0


def test_resource_manager_auto_naming():
    """Test ResourceManager auto-naming for attached contexts."""
    def mock_factory():
        return Mock()
    
    rm = ResourceManager(db_factory=mock_factory, mq_factory=mock_factory)
    
    with rm:
        mock_cm = Mock()
        mock_cm.__enter__ = Mock(return_value="value")
        mock_cm.__exit__ = Mock(return_value=False)
        
        # Attach without explicit name
        rm.attach_context(mock_cm)
        
        metrics = rm.metrics_snapshot()
        # Should have auto-generated name
        auto_names = [name for name in metrics['attached'].keys() if name.startswith('ctx:Mock:')]
        assert len(auto_names) == 1


@pytest.mark.asyncio
async def test_database_manager_concurrent_access():
    """Test DatabaseManager with concurrent access patterns."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        async def worker(worker_id):
            with DatabaseManager(db_path, setup_schema=False) as db:
                # Insert data
                db.execute("INSERT INTO aggregates VALUES (?, ?, ?, ?, ?, ?)", 
                          (worker_id, f'worker_{worker_id}', 100.0 * worker_id, 5, 2, 50.0 * worker_id))
                db.commit()
                return worker_id
        
        # Set up schema first
        with DatabaseManager(db_path) as db:
            pass  # Schema setup happens in __enter__
        
        # Run concurrent workers
        tasks = [worker(i) for i in range(5)]
        results = await asyncio.gather(*tasks)
        
        assert len(results) == 5
        
        # Verify all data was inserted
        with DatabaseManager(db_path, setup_schema=False) as db:
            count = db.execute("SELECT COUNT(*) as count FROM aggregates").fetchone()
            assert count['count'] == 5
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass