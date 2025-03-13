import asyncio
import threading
from asyncio import Queue, Semaphore, Event
from typing import Protocol, TypeVar, Generic, Dict, Any, Tuple, Set
import logging
from datetime import datetime
import time

# Setup basic logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Define the resource protocol
class ResourceProtocol(Protocol):
    name: str
    last_updated: datetime

    async def sync(self) -> 'ResourceProtocol':
        """Sync the resource state and return updated version (polling mode)"""
        ...

    def is_terminal(self) -> bool:
        """Check if resource has reached terminal state"""
        ...

    def is_started(self) -> bool:
        """Check if resource has been started."""
        ...

    async def launch(self) -> bool:
        """Launch the resource, invoked only once for resources that need to be started.
        Returns: True if successfully launched, False otherwise."""
        ...

# Type variable for generic resource
R = TypeVar('R', bound=ResourceProtocol)


class Informer(Generic[R]):
    """Base informer with either polling or watch mode"""

    def __init__(self, shared_queue: Queue, sync_period: float = 5.0, use_watch: bool = False):
        self.sync_period = sync_period if not use_watch else None
        self.resources: Dict[str, R] = {}
        self.shared_queue = shared_queue
        self.running = False
        self.use_watch = use_watch
        self._lock = asyncio.Lock()
        self._watch_task: asyncio.Task = None

    async def add_resource(self, resource: R):
        """Add a new resource to watch"""
        async with self._lock:
            if resource.name not in self.resources:
                self.resources[resource.name] = resource
                logger.info(f"Added resource to informer: {resource.name}")
                await self.shared_queue.put(resource)

    async def remove_resource(self, resource_name: str):
        """Remove a resource from watching"""
        async with self._lock:
            if resource_name in self.resources:
                del self.resources[resource_name]
                logger.info(f"Removed resource from informer: {resource_name}")

    async def _on_resource_update(self, resource: R):
        """Callback for watch-based updates"""
        async with self._lock:
            if resource.name in self.resources:
                self.resources[resource.name] = resource
                await self.shared_queue.put(resource)
            else:
                logger.debug(f"Received update for unknown resource: {resource.name}")

    async def watch(self):
        """Watch for updates on all resources - to be implemented by subclasses for watch mode"""
        raise NotImplementedError("Watch mode requires a custom informer implementation")

    async def start(self):
        """Start the informer"""
        if self.running:
            logger.warning("Informer already running")
            return
        self.running = True
        logger.info(f"Started informer in {'watch' if self.use_watch else 'polling'} mode")
        if self.use_watch:
            self._watch_task = asyncio.create_task(self.watch())
        else:
            asyncio.create_task(self._run_polling())

    async def stop(self):
        """Stop the informer"""
        self.running = False
        if self.use_watch and self._watch_task:
            self._watch_task.cancel()
            self._watch_task = None
        logger.info("Stopped informer")

    async def _run_polling(self):
        """Background resource update loop for polling mode"""
        while self.running:
            try:
                async with self._lock:
                    resources_to_sync = list(self.resources.items())

                for name, resource in resources_to_sync:
                    new_resource = await resource.sync()
                    async with self._lock:
                        if name in self.resources:
                            self.resources[name] = new_resource
                            await self.shared_queue.put(new_resource)

                await asyncio.sleep(self.sync_period)
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                await asyncio.sleep(self.sync_period)

class Controller(Generic[R]):
    """Generic controller with high-level submit API"""

    def __init__(self, informer: Informer[R], shared_queue: Queue, max_concurrent_launches: int = 2):
        self.informer = informer
        self.shared_queue = shared_queue
        self.running = False
        self.max_concurrent_launches = max_concurrent_launches
        self.launch_semaphore = Semaphore(max_concurrent_launches)
        self.launching_tasks: Dict[str, asyncio.Task] = {}
        self.completion_events: Dict[str, Event] = {}  # Track completion events

    @classmethod
    def for_watchable_resource(cls, informer: Informer[R], max_concurrent_launches: int = 2) -> 'Controller[R]':
        if not informer.use_watch:
            raise ValueError("Informer must be in watch mode")
        watch_queue = Queue()
        return cls(informer, watch_queue, max_concurrent_launches)

    @classmethod
    def for_sync_resource(cls, max_concurrent_launches: int = 2) -> 'Controller[R]':
        poll_queue = Queue()
        poll_informer = Informer[R](poll_queue, sync_period=2.0, use_watch=False)
        return cls(poll_informer, poll_queue, max_concurrent_launches)

    async def add_resource(self, resource: R):
        """Public API to add a resource without waiting for completion"""
        print(f"{threading.current_thread().name} Adding resource {resource.name}")
        await self.informer.add_resource(resource)
        await self.shared_queue.put(resource.name)
        print(f"{threading.current_thread().name} Done adding resource {resource.name}")

    async def submit_resource(self, resource: R) -> R:
        """Submit a resource and await its completion, returning the final state"""
        print(f"{threading.current_thread().name} Submitting resource {resource.name}")
        async with self.informer._lock:
            # TODO change informer to have get and other methods. we should never access resources directly
            if resource.name in self.informer.resources:
                raise ValueError(f"Resource {resource.name} already exists")
            if resource.name in self.completion_events:
                raise ValueError(f"Resource {resource.name} is already being processed")

        # Create completion event and add resource
        self.completion_events[resource.name] = Event()
        await self.add_resource(resource)

        print(f"{threading.current_thread().name} Waiting for completion of {resource.name}")
        # Wait for completion
        await self.completion_events[resource.name].wait()
        print(f"{threading.current_thread().name} Resource {resource.name} completed")

        # Get final resource state and clean up
        final_resource = self.informer.resources.get(resource.name)
        if final_resource is None:
            raise ValueError(f"Resource {resource.name} not found")
        del self.completion_events[resource.name]
        print(f"{threading.current_thread().name} Removed completion event for {resource.name}")
        await self.informer.remove_resource(resource.name)
        print(f"{threading.current_thread().name} Removed resource {resource.name}, final={final_resource}")
        return final_resource

    async def launch_resource(self, resource: R):
        """Attempt to launch a resource until successful"""
        async with self.launch_semaphore:
            while not resource.is_started() and self.running:
                logger.info(f"Attempting to launch resource: {resource.name}")
                success = await resource.launch()
                if success:
                    logger.info(f"Successfully launched resource: {resource.name}")
                    await self.shared_queue.put(resource)
                    break
                else:
                    logger.warning(f"Failed to launch resource: {resource.name}, retrying...")
                    await asyncio.sleep(1)

    async def process_resource(self, resource: R):
        """Process resource updates"""
        logger.info(f"Processing resource: name={resource.name}, "
                   f"started={resource.is_started()}")

        if not resource.is_started():
            if resource.name not in self.launching_tasks:
                task = asyncio.create_task(self.launch_resource(resource))
                self.launching_tasks[resource.name] = task
                task.add_done_callback(lambda t: self.launching_tasks.pop(resource.name, None))
        elif resource.is_terminal():
            if resource.name in self.completion_events:
                self.completion_events[resource.name].set()  # Signal completion

    async def get_resource_status(self) -> Tuple[Set[str], Set[str]]:
        """Return current set of launched (running) and waiting-to-be-launched resources"""
        async with self.informer._lock:
            launched = {name for name, res in self.informer.resources.items() if res.is_started()}
            waiting = {name for name, res in self.informer.resources.items() if not res.is_started()}
        return launched, waiting

    async def _log_resource_stats(self):
        """Periodically log resource stats if debug is enabled"""
        while self.running:
            launched, waiting = await self.get_resource_status()
            logger.debug(f"Resource stats: Launched={launched}, Waiting={waiting}")
            await asyncio.sleep(2.0)

    async def run(self):
        """Run loop with resource status logging"""
        print(f"{threading.current_thread().name} Controller starting")
        if self.running:
            logger.warning("Controller already running")
            return

        print(f"{threading.current_thread().name} Controller running")
        self.running = True
        await self.informer.start()
        print(f"{threading.current_thread().name} Informer started")
        asyncio.create_task(self._log_resource_stats())

        while self.running:
            try:
                print(f"{threading.current_thread().name} Waiting for resource")
                item = await self.shared_queue.get()
                print(f"{threading.current_thread().name} Got resource {item}")

                if isinstance(item, str):
                    logger.info(f"Received resource name: {item}")
                    async with self.informer._lock:
                        if item in self.informer.resources:
                            await self.process_resource(self.informer.resources[item])
                else:
                    await self.process_resource(item)

                self.shared_queue.task_done()

            except Exception as e:
                logger.error(f"Error in controller loop: {e}")
                await asyncio.sleep(1.0)

    async def stop(self):
        """Stop the controller"""
        self.running = False
        for task in self.launching_tasks.values():
            task.cancel()
        self.launching_tasks.clear()
        for event in self.completion_events.values():
            event.set()  # Unblock any waiting submit calls
        self.completion_events.clear()
        await self.informer.stop()

# ----------------- DEMO -----------------

class PollingResource:
    def __init__(self, name: str, data: Dict[str, Any] = None):
        self.name = name
        self.data = data or {"status": "initial"}
        self.last_updated = datetime.now()
        self._started = False

    async def sync(self) -> 'PollingResource':
        await asyncio.sleep(0.1)
        current_status = self.data.get("status", "initial")
        if current_status == "initial":
            new_status = "running" if self._started else "initial"
        elif current_status == "running":
            new_status = "completed"
        else:
            new_status = current_status

        return PollingResource(
            name=self.name,
            data={"status": new_status, "timestamp": str(datetime.now())}
        )

    def is_terminal(self) -> bool:
        return self.data.get("status") == "completed"

    def is_started(self) -> bool:
        return self._started

    async def launch(self) -> bool:
        await asyncio.sleep(0.1)
        if not self._started and self.data["status"] == "initial":
            self._started = True
            self.data["status"] = "running"
            self.last_updated = datetime.now()
            return True
        return False

class WatchResource:
    def __init__(self, name: str, data: Dict[str, Any] = None):
        self.name = name
        self.data = data or {"phase": "pending"}
        self.last_updated = datetime.now()
        self._started = False

    async def sync(self) -> 'WatchResource':
        raise NotImplementedError("Sync not supported for watch-based resource")

    def is_terminal(self) -> bool:
        return self.data.get("phase") == "done"

    def is_started(self) -> bool:
        return self._started

    async def launch(self) -> bool:
        await asyncio.sleep(0.1)
        if not self._started and self.data["phase"] == "pending":
            if time.time() % 2 < 1:  # Fail half the time
                return False
            self._started = True
            self.data["phase"] = "active"
            self.last_updated = datetime.now()
            return True
        return False

class WatchInformer(Informer[WatchResource]):
    async def watch(self):
        """Simulated watch implementation for all resources"""
        while self.running:
            await asyncio.sleep(1.0)
            async with self._lock:
                resources = list(self.resources.items())

            for name, resource in resources:
                if resource.is_started():
                    states = ["active", "done"]
                    current_idx = states.index(resource.data["phase"]) if resource.data["phase"] in states else -1
                    if current_idx < len(states) - 1:
                        current_idx += 1
                        updated = WatchResource(
                            name=name,
                            data={"phase": states[current_idx], "time": str(datetime.now())}
                        )
                        updated._started = True
                        await self._on_resource_update(updated)

async def main():
    # Enable debug logging
    logger.setLevel(logging.DEBUG)

    # Polling-based controller
    poll_controller = Controller.for_sync_resource(max_concurrent_launches=2)

    # Watch-based controller
    watch_queue = Queue()
    watch_informer = WatchInformer(watch_queue, use_watch=True)
    watch_controller = Controller.for_watchable_resource(watch_informer, max_concurrent_launches=2)

    # Start both controllers
    poll_task = asyncio.create_task(poll_controller.run())
    watch_task = asyncio.create_task(watch_controller.run())

    # Submit resources and await completion
    async def submit_and_log(controller: Controller, resource: ResourceProtocol):
        logger.info(f"Submitting {resource.name}")
        final_resource = await controller.submit_resource(resource)
        logger.info(f"Completed {resource.name}: {final_resource.data if final_resource else 'None'}")

    await asyncio.gather(
        submit_and_log(poll_controller, PollingResource("poll-1")),
        submit_and_log(watch_controller, WatchResource("watch-1")),
        submit_and_log(poll_controller, PollingResource("poll-2")),
        submit_and_log(watch_controller, WatchResource("watch-2")),
        submit_and_log(poll_controller, PollingResource("poll-3")),
        submit_and_log(watch_controller, WatchResource("watch-3"))
    )

    # Cleanup
    await poll_controller.stop()
    await watch_controller.stop()
    poll_task.cancel()
    watch_task.cancel()
    try:
        await asyncio.gather(poll_task, watch_task)
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    asyncio.run(main())