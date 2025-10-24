import base64

class FirestoreSerializer:
    def __init__(self, serde):
        self.serde = serde
    
    def dumps_typed(self, obj):
        type_, data = self.serde.dumps_typed(obj)
        data_base64 = base64.b64encode(data).decode('utf-8')
        return type_, data_base64

    def loads_typed(self, data):
        type_name, serialized_obj = data
        serialized_obj = base64.b64decode(serialized_obj.encode('utf-8'))
        return self.serde.loads_typed((type_name, serialized_obj))
