from typing import Any, Dict


def serialize(data: Dict[str, Any]) -> Dict[str, Any]:
    """Serialize a dictionary into a format suitable for DynamoDB."""
    serialized_data = {}
    for key, value in data.items():
        if isinstance(value, str):
            serialized_data[key] = {"S": value}
        elif isinstance(value, int):
            serialized_data[key] = {"N": str(value)}
        elif isinstance(value, float):
            serialized_data[key] = {"N": str(value)}
        elif isinstance(value, bool):
            serialized_data[key] = {"BOOL": value}
        elif isinstance(value, dict):
            serialized_data[key] = {"M": serialize(value)}
        elif isinstance(value, list):
            serialized_data[key] = {
                "L": [serialize(v) if isinstance(v, dict) else v for v in value]
            }
        elif value is None:
            serialized_data[key] = {"NULL": True}
        else:
            raise TypeError(f"Unsupported data type: {type(value)} for key {key}")
    return serialized_data


def deserialize(data: Dict[str, Any]) -> Dict[str, Any]:
    """Deserialize a DynamoDB item into a dictionary."""
    deserialized_data = {}
    for key, value in data.items():
        if "S" in value:
            deserialized_data[key] = value["S"]
        elif "N" in value:
            deserialized_data[key] = (
                int(value["N"]) if value["N"].isdigit() else float(value["N"])
            )
        elif "BOOL" in value:
            deserialized_data[key] = value["BOOL"]
        elif "M" in value:
            deserialized_data[key] = deserialize(value["M"])
        elif "L" in value:
            deserialized_data[key] = [
                deserialize(v) if isinstance(v, dict) else v for v in value["L"]
            ]
        elif "NULL" in value:
            deserialized_data[key] = None
        else:
            raise TypeError(f"Unsupported data type for key {key}")
    return deserialized_data
