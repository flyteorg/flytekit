from dataclasses import dataclass
from dataclasses_json import DataClassJsonMixin  # dataclasses-json
from mashumaro import DataClassDictMixin         # mashumaro
from mashumaro.codecs.json import JSONEncoder, JSONDecoder


@dataclass
class User(DataClassJsonMixin, DataClassDictMixin):
    id: int


def main():
    user = User(id=42)

    print("=== Mashumaro serialize (codec) ===")
    enc = JSONEncoder(User)
    s = enc.encode(user)
    print("Serialized:", s)

    print("\n=== Mashumaro deserialize (codec) ===")
    dec = JSONDecoder(User)
    u2 = dec.decode(s)
    print("Deserialized:", u2, "type:", type(u2))

    print("\n=== dataclasses-json still exists (separate API) ===")
    print("dataclasses-json to_json():", user.to_json())


if __name__ == "__main__":
    main()
