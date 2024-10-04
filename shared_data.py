# shared_data.py

from multiprocessing.managers import BaseManager
from multiprocessing import Manager

class SharedDataManager(BaseManager):
    pass

# Function to start the manager server
def start_manager():
    # Create a multiprocessing Manager
    m = Manager()

    # Create managed dictionaries
    tasks_progress = m.dict()
    tasks_result = m.dict()

    # Register methods to access the managed dictionaries
    SharedDataManager.register('get_tasks_progress', callable=lambda: tasks_progress)
    SharedDataManager.register('get_tasks_result', callable=lambda: tasks_result)

    # Start the manager server
    manager = SharedDataManager(address=('', 50000), authkey=b'secret')
    server = manager.get_server()
    print("Manager server started")
    server.serve_forever()

# Function to get the manager for client processes
def get_manager():
    # Register the methods without callable for the client processes
    SharedDataManager.register('get_tasks_progress')
    SharedDataManager.register('get_tasks_result')

    # Connect to the manager server
    manager = SharedDataManager(address=('localhost', 50000), authkey=b'secret')
    manager.connect()
    return manager
