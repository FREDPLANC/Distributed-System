import unittest
from unittest.mock import MagicMock, patch, call, mock_open, AsyncMock
import subprocess
import asyncio
import threading
import sys
from datetime import datetime
import os
import socket
from mp1 import (
    handle_client,
    client_task,
    server_task,
    main
)
# Assume the code above has been imported and the functions are available in the current namespace


class TestHandleClient(unittest.TestCase):
    def test_handle_client_with_matching_grep(self):
        # Arrange
        mock_client_socket = MagicMock()
        mock_client_socket.recv.return_value = b'grep test_pattern'
        
        with patch('socket.gethostname', return_value='a24-cs425-a005.cs.illinois.edu'):
            with patch('subprocess.check_output', return_value='matching line\n'):
                with patch('builtins.print') as mock_print:
                    # Act
                    handle_client(mock_client_socket)
                    # Assert
                    mock_client_socket.sendall.assert_called_once_with('matching line\n'.encode())
                    mock_client_socket.close.assert_called_once()
                    mock_print.assert_called_with("Enter 'grep <pattern>' to search logs or 'exit' to quit: ")

    def test_handle_client_with_no_matching_grep(self):
        # Arrange
        mock_client_socket = MagicMock()
        mock_client_socket.recv.return_value = b'grep test_pattern'

        with patch('socket.gethostname', return_value='a24-cs425-a005.cs.illinois.edu'):
            with patch('subprocess.check_output', side_effect=subprocess.CalledProcessError(1, 'grep')):
                with patch('builtins.print') as mock_print:
                    # Act
                    handle_client(mock_client_socket)

                    # Assert
                    mock_client_socket.sendall.assert_called_once_with(b'No matches found')
                    mock_client_socket.close.assert_called_once()
                    mock_print.assert_called_with("Enter 'grep <pattern>' to search logs or 'exit' to quit: ")

    def test_handle_client_with_invalid_request(self):
        # Arrange
        mock_client_socket = MagicMock()
        mock_client_socket.recv.return_value = b'invalid_command'

        with patch('socket.gethostname', return_value='a24-cs425-a005.cs.illinois.edu'):
            with patch('builtins.print') as mock_print:
                # Act
                handle_client(mock_client_socket)

                # Assert
                mock_client_socket.sendall.assert_not_called()
                mock_client_socket.close.assert_called_once()
                mock_print.assert_called_with("Enter 'grep <pattern>' to search logs or 'exit' to quit: ")

    def test_handle_client_with_connection_reset_during_send(self):
        # Arrange
        mock_client_socket = MagicMock()
        mock_client_socket.recv.return_value = b'grep test_pattern'

        with patch('socket.gethostname', return_value='a24-cs425-a005.cs.illinois.edu'):
            with patch('subprocess.check_output', return_value='matching line\n'):
                with patch('builtins.print') as mock_print:
                    # Configure mock to raise ConnectionResetError during sendall
                    def sendall_side_effect(*args, **kwargs):
                        raise ConnectionResetError

                    mock_client_socket.sendall.side_effect = sendall_side_effect

                    # Act
                    handle_client(mock_client_socket)

                    # Assert
                    mock_client_socket.close.assert_called_once()
                    mock_print.assert_any_call("Connection reset by peer, stopping transmission.")
                    mock_print.assert_called_with("Enter 'grep <pattern>' to search logs or 'exit' to quit: ")

class TestClientTask(unittest.IsolatedAsyncioTestCase):
    async def test_client_task_successful(self):
        # Arrange
        # hosts = ['a24-cs425-a001.cs.illinois.edu', 'a24-cs425-a002.cs.illinois.edu', 'a24-cs425-a003.cs.illinois.edu']
        # hosts = ['172.22.159.77', '172.22.95.76']
        hosts = ['172.22.159.77', '172.22.95.76']
        port = 20000
        grep_pattern = 'test_pattern'

        # Mock subprocess.check_output for local grep
        with patch('subprocess.check_output', return_value='local matching line\n') as mock_subproc:
            # Mock socket.gethostname()
            with patch('socket.gethostname', return_value='a24-cs425-a001.cs.illinois.edu'):
                #Mock socket.gethostbyname()
                with patch('socket.gethostbyname', return_value='172.22.157.76'):
                # Mock os.makedirs
                    with patch('os.makedirs') as mock_makedirs:
                        # Mock datetime.now()
                        with patch('mp1.datetime') as mock_datetime:
                            mock_datetime.now.return_value = datetime(2021, 1, 1, 12, 0, 0)
                            timestamp = '20210101_120000'
                            # Mock asyncio.open_connection
                            async def mock_open_connection(host, port):

                                mock_reader = AsyncMock()
                                mock_writer = MagicMock()


                                mock_reader.read = AsyncMock(side_effect=[
                                    b'matching line from %s\n' % host.encode(),  # 
                                    b''  #
                                ])


                                mock_writer.write = MagicMock()  
                                mock_writer.drain = AsyncMock() 
                                mock_writer.close = MagicMock()  
                                mock_writer.wait_closed = AsyncMock()  

                                return mock_reader, mock_writer

                            # async def mock_open_connection(host, port):
                            #     # Return mock reader and writer
                            #     mock_reader = asyncio.StreamReader()
                            #     mock_writer = MagicMock()

                            #     # Simulate the server sending data
                            #     data = f'matching line from {host}\n'

                            #     # Feed data into the reader
                            #     mock_reader.feed_data(data.encode())
                            #     mock_reader.feed_eof()
                            #     # Simulate writer methods
                            #     mock_writer.write = MagicMock()
                            #     mock_writer.drain = MagicMock()
                            #     mock_writer.close = MagicMock()
                            #     mock_writer.wait_closed = MagicMock()
                            #     return mock_reader, mock_writer

                            with patch('asyncio.open_connection', side_effect=mock_open_connection) as mock_conn:
                                with patch('builtins.open', new_callable=mock_open) as mock_file:
                                    # print("adas")
                                    with patch('builtins.print') as mock_print:
                                        # Act
                                        await client_task(hosts, port, grep_pattern)

                                        # Assert
                                        # Check that local grep was called
                                        mock_subproc.assert_called_with('grep test_pattern vm1.log', shell=True, text=True)

                                        # Check that open_connection was called for each host
                                        expected_calls = [call(host, port) for host in hosts]
                                        self.assertEqual(mock_conn.call_args_list, expected_calls)

                                        expected_open_calls = [
                                            call(os.path.join(timestamp, 'vm1.log'), 'w'),
                                            call(os.path.join(timestamp, 'vm2.log'), 'w'),
                                            call(os.path.join(timestamp, 'vm3.log'), 'w'),
                                        ]
                                        

                                        self.assertEqual(mock_file.call_args_list, expected_open_calls)

                                        # Check that data was written to files
                                        expected_write_calls = [
                                            call().write(f'matching line from {hosts[0]}\n'),
                                            call().write(f'matching line from {hosts[1]}\n'),
                                            call().write('local matching line\n'),
                                        ]
                                        actual_write_calls = mock_file().write.call_args_list
                                        # Extract the written data
                                        written_data = [args[0] for args, _ in actual_write_calls]
                                        expected_data = [
                                            f'local matching line\n',
                                            f'matching line from {hosts[0]}\n',
                                            f'matching line from {hosts[1]}\n',
                                            
                                        ]
                                        self.assertEqual(written_data, expected_data)

                                            # Check that prints were called
                                        expected_print_calls = [
                                            call(f'VM1:1 matching lines from 172.22.157.76'),
                                            call(f'VM2:1 matching lines from {hosts[0]}'),
                                            call(f'VM3:1 matching lines from {hosts[1]}'),
                                            call('Summary: the total line count is: 3')
                                        ]
                                        self.assertEqual(mock_print.call_args_list, expected_print_calls)

    async def test_client_task_with_timeout(self):
        # Arrange
        hosts = ['172.22.157.76']
        port = 20000
        grep_pattern = 'test_pattern'

        # Mock subprocess.check_output for local grep
        async def mock_open_connection(host, port):
            raise asyncio.TimeoutError
        with patch('asyncio.open_connection', side_effect=mock_open_connection):
        # with patch('mp1.client_task.handle_host', new=AsyncMock(side_effect=mock_open_connection)):
            with patch('subprocess.check_output', return_value='local matching line\n'):
                # Mock socket.gethostname()
                with patch('socket.gethostname', return_value='a24-cs425-a001.cs.illinois.edu'):
                    with patch('socket.gethostbyname', return_value='172.22.157.76'):
                    # Mock os.makedirs
                        # with patch('os.makedirs'):
                            # Mock datetime.now()
                        with patch('datetime.datetime') as mock_datetime:
                            mock_datetime.now.return_value = datetime(2021, 1, 1, 12, 0, 0)
                            # Mock asyncio.open_connection to raise TimeoutError
                            
                            with patch('builtins.print') as mock_print:
                                # Act
                                await client_task(hosts, port, grep_pattern)

                                # Assert
                                # Check that prints indicate a timeout
                                expected_calls = [
                                        call('Connection timeout occurred for 172.22.157.76. Skipping...'),
                                        # call('VM1: Failed to retrieve logs Timeout'),
                                        # call('Summary: the total line count is: 1')
                                    ]
                                mock_print.assert_has_calls(expected_calls, any_order=False)


if __name__ == '__main__':
    print("running")
    unittest.main(buffer=False)