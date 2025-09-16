import os
import sys
import pytest
import asyncio
import json
import tempfile
from unittest.mock import Mock, patch, AsyncMock
from aiohttp.test_utils import AioHTTPTestCase, make_mocked_request
from aiohttp import web, ClientTimeout
import aiohttp.test_utils

# Ensure repository root is on path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Import after path setup
sys.path.insert(0, os.path.join(REPO_ROOT, 'etl_pipeline'))
from etl_pipeline.webhook import handle_webhook, healthz, metrics, on_startup, on_cleanup, _validate_payload
from etl_pipeline.pipeline import Pipeline


class TestWebhookValidation:
    """Test webhook payload validation."""
    
    def test_validate_payload_valid(self):
        """Test validation of valid payloads."""
        valid_payload = {
            "data": {
                "category": "electronics",
                "value": 100.5,
                "quantity": 2
            }
        }
        assert _validate_payload(valid_payload) is None
    
    def test_validate_payload_minimal_valid(self):
        """Test validation of minimal valid payload."""
        minimal_payload = {
            "data": {
                "category": "books"
            }
        }
        assert _validate_payload(minimal_payload) is None
    
    def test_validate_payload_invalid_structure(self):
        """Test validation rejects invalid structure."""
        # Missing data field
        assert _validate_payload({}) == "Invalid payload structure"
        assert _validate_payload({"data": "not_dict"}) == "Invalid payload structure"
        assert _validate_payload("not_dict") == "Invalid payload structure"
    
    def test_validate_payload_missing_category(self):
        """Test validation rejects missing category."""
        payload = {"data": {"value": 100}}
        assert _validate_payload(payload) == "Missing or invalid category"
        
        payload = {"data": {"category": 123}}  # Non-string category
        assert _validate_payload(payload) == "Missing or invalid category"
    
    def test_validate_payload_invalid_types(self):
        """Test validation rejects invalid field types."""
        # Invalid value type
        payload = {"data": {"category": "test", "value": "not_number"}}
        assert _validate_payload(payload) == "Invalid value type"
        
        # Invalid quantity type
        payload = {"data": {"category": "test", "quantity": "not_int"}}
        assert _validate_payload(payload) == "Invalid quantity type"
        
        payload = {"data": {"category": "test", "quantity": 3.14}}  # Float not int
        assert _validate_payload(payload) == "Invalid quantity type"


class TestWebhookEndpoints(AioHTTPTestCase):
    """Test webhook HTTP endpoints."""
    
    async def get_application(self):
        """Create test application."""
        app = web.Application()
        
        # Create mock pipeline
        mock_pipeline = Mock(spec=Pipeline)
        mock_pipeline.ingest_q = AsyncMock()
        mock_pipeline.get_queue_info.return_value = {
            "ingest_q": 0, "db_q": 0, "outbound_q": 0, 
            "db_failures_q": 0, "dead_letter_q": 0
        }
        mock_pipeline.get_resource_metrics.return_value = {
            "active": True,
            "acquire_ms": {"db": 10.5, "mq": 5.2},
            "context_ms": 150.0,
            "attached": {}
        }
        mock_pipeline._tasks = []
        
        app["pipeline_instance"] = mock_pipeline
        
        # Add routes
        app.router.add_post("/webhook", handle_webhook)
        app.router.add_get("/healthz", healthz)
        app.router.add_get("/metrics", metrics)
        
        return app
    
    async def test_webhook_valid_payload(self):
        """Test webhook with valid payload."""
        payload = {
            "data": {
                "category": "electronics",
                "value": 99.99,
                "quantity": 1
            }
        }
        
        resp = await self.client.request("POST", "/webhook", json=payload)
        assert resp.status == 202
        assert await resp.text() == "Accepted"
        
        # Verify pipeline was called
        self.app["pipeline_instance"].ingest_q.put.assert_called_once_with(payload)
    
    async def test_webhook_invalid_json(self):
        """Test webhook with invalid JSON."""
        resp = await self.client.request("POST", "/webhook", data="invalid json")
        assert resp.status == 400
        assert await resp.text() == "Invalid JSON"
    
    async def test_webhook_invalid_payload_structure(self):
        """Test webhook with invalid payload structure."""
        resp = await self.client.request("POST", "/webhook", json={})
        assert resp.status == 422
        assert await resp.text() == "Invalid payload structure"
    
    async def test_webhook_missing_category(self):
        """Test webhook with missing category."""
        payload = {"data": {"value": 100}}
        resp = await self.client.request("POST", "/webhook", json=payload)
        assert resp.status == 422
        assert await resp.text() == "Missing or invalid category"
    
    async def test_webhook_invalid_value_type(self):
        """Test webhook with invalid value type."""
        payload = {"data": {"category": "test", "value": "not_a_number"}}
        resp = await self.client.request("POST", "/webhook", json=payload)
        assert resp.status == 422
        assert await resp.text() == "Invalid value type"
    
    async def test_webhook_timeout(self):
        """Test webhook timeout handling."""
        # Mock pipeline to timeout
        self.app["pipeline_instance"].ingest_q.put = AsyncMock(
            side_effect=asyncio.TimeoutError()
        )
        
        payload = {"data": {"category": "test"}}
        resp = await self.client.request("POST", "/webhook", json=payload)
        assert resp.status == 429
        assert await resp.text() == "Busy, try again soon"
    
    async def test_healthz_healthy(self):
        """Test health check endpoint when healthy."""
        resp = await self.client.request("GET", "/healthz")
        assert resp.status == 200
        
        data = await resp.json()
        assert data["status"] == "healthy"
        assert "queues" in data
        assert "resources" in data
        assert "workers" in data
        assert data["resources"]["active"] is True
    
    async def test_healthz_unhealthy(self):
        """Test health check endpoint when unhealthy."""
        # Mock pipeline as inactive
        self.app["pipeline_instance"].get_resource_metrics.return_value = {
            "active": False,
            "acquire_ms": {"db": None, "mq": None},
            "context_ms": None,
            "attached": {}
        }
        
        resp = await self.client.request("GET", "/healthz")
        assert resp.status == 503
        
        data = await resp.json()
        assert data["status"] == "unhealthy"
        assert data["resources"]["active"] is False
    
    async def test_metrics_endpoint(self):
        """Test metrics endpoint."""
        resp = await self.client.request("GET", "/metrics")
        assert resp.status == 200
        
        data = await resp.json()
        assert "timestamp" in data
        assert "resource_manager" in data
        assert "queues" in data
        assert "pipeline" in data
        assert "workers" in data
        
        # Verify structure
        assert data["resource_manager"]["active"] is True
        assert "acquire_ms" in data["resource_manager"]
        assert "db" in data["resource_manager"]["acquire_ms"]
        assert "mq" in data["resource_manager"]["acquire_ms"]


class TestWebhookLifecycle:
    """Test webhook application lifecycle."""
    
    @pytest.mark.asyncio
    async def test_startup_and_cleanup(self):
        """Test application startup and cleanup handlers."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            app = web.Application()
            
            # Mock environment variables
            with patch.dict(os.environ, {
                'QUEUE_MAXSIZE': '50',
                'WINDOW_SECS': '5',
                'DB_PATH': db_path
            }):
                # Test startup
                await on_startup(app)
                
                # Verify pipeline was created and started
                assert "pipeline_instance" in app
                assert "pipeline_tasks" in app
                
                pipeline = app["pipeline_instance"]
                assert pipeline is not None
                assert hasattr(pipeline, 'get_queue_info')
                assert hasattr(pipeline, 'get_resource_metrics')
                
                # Test cleanup
                await on_cleanup(app)
                
                # Pipeline should be cleaned up
                # (Note: In real implementation, this would stop workers and close resources)
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass
    
    @pytest.mark.asyncio
    async def test_startup_failure_cleanup(self):
        """Test cleanup on startup failure."""
        app = web.Application()
        
        # Mock pipeline creation to fail
        with patch('etl_pipeline.webhook.Pipeline') as mock_pipeline_class:
            mock_pipeline = Mock()
            mock_pipeline.__enter__ = Mock(side_effect=RuntimeError("Startup failed"))
            mock_pipeline.__exit__ = Mock()
            mock_pipeline_class.return_value = mock_pipeline
            
            with pytest.raises(RuntimeError, match="Startup failed"):
                await on_startup(app)
            
            # Verify cleanup was called
            mock_pipeline.__exit__.assert_called_once()


class TestWebhookIntegration:
    """Integration tests for webhook with real pipeline components."""
    
    @pytest.mark.asyncio
    async def test_webhook_with_real_pipeline(self):
        """Test webhook integration with real pipeline."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # Create real pipeline
            pipeline = Pipeline(
                queue_maxsize=10,
                window_secs=2,
                db_path=db_path
            )
            
            with pipeline:
                app = web.Application()
                app["pipeline_instance"] = pipeline
                app.router.add_post("/webhook", handle_webhook)
                
                # Start pipeline workers
                tasks = await pipeline.start_workers()
                
                try:
                    # Create test client
                    async with aiohttp.test_utils.TestClient(
                        aiohttp.test_utils.TestServer(app)
                    ) as client:
                        # Send test payload
                        payload = {
                            "data": {
                                "category": "integration_test",
                                "value": 123.45,
                                "quantity": 2
                            }
                        }
                        
                        resp = await client.post("/webhook", json=payload)
                        assert resp.status == 202
                        
                        # Wait a bit for processing
                        await asyncio.sleep(0.1)
                        
                        # Check queue info
                        queue_info = pipeline.get_queue_info()
                        # Queue should have processed the message (or be processing it)
                        assert isinstance(queue_info, dict)
                        assert "ingest_q" in queue_info
                
                finally:
                    # Clean up workers
                    await pipeline.stop_workers(timeout=1.0)
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass
    
    @pytest.mark.asyncio
    async def test_webhook_stress_test(self):
        """Test webhook under load."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            pipeline = Pipeline(
                queue_maxsize=100,
                window_secs=1,
                db_path=db_path
            )
            
            with pipeline:
                app = web.Application()
                app["pipeline_instance"] = pipeline
                app.router.add_post("/webhook", handle_webhook)
                
                tasks = await pipeline.start_workers()
                
                try:
                    async with aiohttp.test_utils.TestClient(
                        aiohttp.test_utils.TestServer(app)
                    ) as client:
                        # Send multiple requests concurrently
                        async def send_request(i):
                            payload = {
                                "data": {
                                    "category": f"stress_test_{i % 5}",
                                    "value": i * 10.0,
                                    "quantity": 1
                                }
                            }
                            resp = await client.post("/webhook", json=payload)
                            return resp.status
                        
                        # Send 20 concurrent requests
                        results = await asyncio.gather(*[send_request(i) for i in range(20)])
                        
                        # Most should succeed (some might get 429 if queue fills up)
                        success_count = sum(1 for status in results if status == 202)
                        assert success_count >= 10  # At least half should succeed
                
                finally:
                    await pipeline.stop_workers(timeout=2.0)
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass


class TestWebhookEdgeCases:
    """Test webhook edge cases and error conditions."""
    
    @pytest.mark.asyncio
    async def test_webhook_large_payload(self):
        """Test webhook with large payload."""
        # Create a large but valid payload
        large_category = "x" * 1000  # 1KB category name
        payload = {
            "data": {
                "category": large_category,
                "value": 99.99,
                "quantity": 1
            }
        }
        
        mock_pipeline = Mock(spec=Pipeline)
        mock_pipeline.ingest_q = AsyncMock()
        
        app = web.Application()
        app["pipeline_instance"] = mock_pipeline
        app.router.add_post("/webhook", handle_webhook)
        
        async with aiohttp.test_utils.TestClient(
            aiohttp.test_utils.TestServer(app)
        ) as client:
            resp = await client.post("/webhook", json=payload)
            assert resp.status == 202
    
    @pytest.mark.asyncio
    async def test_webhook_unicode_handling(self):
        """Test webhook with Unicode characters."""
        payload = {
            "data": {
                "category": "électronique_测试_🔌",
                "value": 99.99,
                "quantity": 1
            }
        }
        
        mock_pipeline = Mock(spec=Pipeline)
        mock_pipeline.ingest_q = AsyncMock()
        
        app = web.Application()
        app["pipeline_instance"] = mock_pipeline
        app.router.add_post("/webhook", handle_webhook)
        
        async with aiohttp.test_utils.TestClient(
            aiohttp.test_utils.TestServer(app)
        ) as client:
            resp = await client.post("/webhook", json=payload)
            assert resp.status == 202
    
    @pytest.mark.asyncio
    async def test_webhook_numeric_edge_cases(self):
        """Test webhook with numeric edge cases."""
        edge_cases = [
            {"value": 0.0, "quantity": 1},           # Zero value
            {"value": -10.5, "quantity": 1},         # Negative value
            {"value": 1e10, "quantity": 1},          # Very large value
            {"value": 1e-10, "quantity": 1},         # Very small value
            {"value": 99.99, "quantity": 0},         # Zero quantity
            {"value": 99.99, "quantity": 1000000},   # Large quantity
        ]
        
        mock_pipeline = Mock(spec=Pipeline)
        mock_pipeline.ingest_q = AsyncMock()
        
        app = web.Application()
        app["pipeline_instance"] = mock_pipeline
        app.router.add_post("/webhook", handle_webhook)
        
        async with aiohttp.test_utils.TestClient(
            aiohttp.test_utils.TestServer(app)
        ) as client:
            for case in edge_cases:
                payload = {"data": {"category": "test", **case}}
                resp = await client.post("/webhook", json=payload)
                assert resp.status == 202, f"Failed for case: {case}"