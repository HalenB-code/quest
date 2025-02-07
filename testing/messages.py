import random
import json
import re

class Message:

    message_log = 0
    requested_nodes = []
    first_node = None

    def __init__(self, nodes: list):
        self.requested_nodes = nodes
    
    def camel_to_snake(camel_str: str):
        snake_str = re.sub('([A-Z])', r'_\1', camel_str).lower()
        return snake_str

    def create_message(self, msg_type):
        msg = {'src': '', 'dst': '', 'body': {'msg_id': '', 'type': ''}}

        # Retrieve message fields
        msg['src'] = self.requested_nodes[random.randint(0, len(self.requested_nodes) - 1)]
        msg['dst'] = self.requested_nodes[random.randint(0, len(self.requested_nodes) - 1)]
        msg['body']['type'] = self.camel_to_snake(msg_type)
        msg_fields['msg_id'] = self.message_log + 1
        self.message_log += 1

        msg_fields = message_type[msg_type]
        
        # Add default values to messages that have them
        if message_type_defaults.get(msg_type) is not None:
            msg_defaults = message_type_defaults.get(msg_type)

            # Overwrite sub-fields with retrieved default values
            for (msg_default_field, msg_default_value) in msg_defaults.items():
                msg_fields[msg_default_field] = msg_default_value

        # Add sub-fields to message
        for msg_field in msg_fields:
            msg['body'][msg_field] = msg_fields[msg_field]

        return json.dumps(msg)
    
    def get_message_type(self, msg):
        return message_type[msg['type']]
    
    def build_node(self, node_id: str):
        msg = {'type': ''}
        msg['type'] = 'Init'.lower()
        msg_fields = message_type['Init']

        msg_fields['msg_id'] = self.message_log + 1
        self.message_log += 1

        msg_fields['node_id'] = node_id
        msg_fields['node_ids'] = [x for x in self.requested_nodes if x != node_id]

        for msg_field in msg_fields:
            msg[msg_field] = msg_fields[msg_field]

        return json.dumps(msg)

    
    def build_messages_ordered(self, iterations: int):
        messages = []

        # Init nodes
        for node in self.requested_nodes:
            messages.append(self.build_node(node))

        for iteration in range(iterations):

            # Loop through message_order list exactly 1 time
            for (idx, msg_type) in message_order.items():

                # If idx is odd and less than 8 - for initial testing
                if (idx % 2 > 0) & (idx < 8):

                    messages.append(self.create_message(msg_type))

                else:
                    break

        return messages
    
    # def build_messages_unordered(self, msg):


message_order = {
    0: 'Init',
    1: 'Echo',
    2: 'EchoOk',
    3: 'Generate',
    4: 'GenerateOk',
    5: 'Broadcast',
    6: 'BroadcastOk',
    7: 'BroadcastRead',
    8: 'BroadcastReadOk',
    9: 'Topology',
    10: 'TopologyOk',
    11: 'VectorAdd',
    12: 'VectorAddOk',
    13: 'VectorRead',
    14: 'VectorReadOk',
    15: 'Send',
    16: 'SendOk',
    17: 'Poll',
    18: 'PollOk',
    19: 'CommitOffsets',
    20: 'CommitOffsetsOk',
    21: 'ListCommitedOffsets',
    22: 'ListCommitedOffsetsOk',
    23: 'Transaction',
    24: 'TransactionOk',
    25: 'KeyValueRead',
    26: 'KeyValueReadOk',
    27: 'KeyValueWrite',
    28: 'KeyValueWriteOk',
    29: 'GlobalCounterRead',
    30: 'GlobalCounterReadOk',
    31: 'GlobalCounterWrite',
    32: 'GlobalCounterWriteOk',
    33: 'ReadFromFile',
    34: 'ReadFromFileOk',
    35: 'DisplayDf',
    36: 'DisplayDfOk'
}

message_type_defaults = {
    'Echo': {
        'echo': 'py-test'
    },
    'Broadcast': {
        'message': 'py-test-broadcast'
    },
    'Topology': {
        'topology': ''
    },
    'VectorAdd': {
        'delta': ''
    },   
    'Send': {
        'key': '',
        'msg': ''
    },   
    'Poll': {
        'offsets': ''
    },
    'CommitOffsets': {
        'offsets': ''
    },
    'ListCommitedOffsets': {
        'keys': ''
    },
    'Transaction': {
        'txn': ''
    },
    'KeyValueRead': {
        'key': ''
    },
    'KeyValueWrite': {
        'key': '',
        'value': ''
    },
    'GlobalCounterRead': {
        'key': '',
        'value': ''
    },
    'GlobalCounterWrite': {
        'key': '',
        'value': ''
    },
    'ReadFromFile': {
        'file_path': '',
        'accessibility': '',
        'bytes': '',
        'schema': ''
    },
    'DisplayDf': {
        'df_name': '',
        'total_rows': ''
    }
}

message_type = {
    'Init': {
        'msg_id': '',
        'node_id': '',
        'node_ids': ''
    },
    'Echo': {
        'msg_id': '',
        'echo': ''},
    'EchoOk': {
        'msg_id': '',
        'in_reply_to': '',
        'echo': '',
    },
    'Generate': {
    },
    'GenerateOk': {
        'msg_id': '',
        'in_reply_to': '',
        'id': ''
    },
    'Broadcast': {
        'message': ''
    },
    'BroadcastOk': {
        'msg_id': '',
        'in_reply_to': '',
    },
    'BroadcastRead': {
    },
    'BroadcastReadOk': {
        'messages': ''
    },
    'Topology': {
        'topology': ''
    },
    'TopologyOk': {
    },
    'VectorAdd': {
        'delta': ''
    },
    'VectorAddOk': {
    },
    'VectorRead': {
    },
    'VectorReadOk': {
        'value': ''
    },    
    'Send': {
        'key': '',
        'msg': ''
    },
    'SendOk': {
        'offset': ''
    },    
    'Poll': {
        'offsets': ''
    },
    'PollOk': {
        'msgs': ''
    },
    'CommitOffsets': {
        'offsets': ''
    },
    'CommitOffsetsOk': {
    },
    'ListCommitedOffsets': {
        'keys': ''
    },
    'ListCommitedOffsetsOk': {
        'offsets': ''
    },
    'Transaction': {
        'txn': ''
    },
    'TransactionOk': {
        'txn': ''
    },
    'KeyValueRead': {
        'key': ''
    },
    'KeyValueReadOk': {
    },
    'KeyValueWrite': {
        'key': '',
        'value': ''
    },
    'KeyValueWriteOk': {
    },
    'GlobalCounterRead': {
        'key': '',
        'value': ''
    },
    'GlobalCounterReadOk': {
    },
    'GlobalCounterWrite': {
        'key': '',
        'value': ''
    },
    'GlobalCounterWriteOk': {
    },
    'ReadFromFile': {
        'file_path': '',
        'accessibility': '',
        'bytes': '',
        'schema': ''
    },
    'ReadFromFileOk': {
    },
    'DisplayDf': {
        'df_name': '',
        'total_rows': ''
    },
    'DisplayDfOk': {
    }
}