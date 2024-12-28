import time

class AppendIdentifier:
    @staticmethod
    def encode(node_id, timestamp=None):
        if timestamp is None:
            timestamp = int(time.time() * 1000)  
        timestamp_str = AppendIdentifier._base36_encode(timestamp)
        node_id_str = AppendIdentifier._base36_encode(node_id)
    
        return f"{timestamp_str}_{node_id_str}"

    @staticmethod
    def decode(encoded_str):
        timestamp_str, node_id_str = encoded_str.split("_")
        timestamp = AppendIdentifier._base36_decode(timestamp_str)
        node_id = AppendIdentifier._base36_decode(node_id_str)
        return node_id, timestamp

    @staticmethod
    def _base36_encode(number):
        chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        result = ""
        while number:
            number, i = divmod(number, 36)
            result = chars[i] + result
        return result or "0"

    @staticmethod
    def _base36_decode(encoded_str):
        return int(encoded_str, 36)

