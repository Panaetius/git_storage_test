import msgpack
import pygit2
import os
import uuid
import random
import string
import datetime
from urllib.parse import quote, unquote


class GitStorage:
    schema = None

    def encode(self, obj):
        if isinstance(obj, datetime.datetime):
            return {"__datetime__": True, "as_str": obj.strftime("%Y%m%dT%H:%M:%S.%f")}
        return obj

    @classmethod
    def decode(cls, obj):
        if "__datetime__" in obj:
            obj = datetime.datetime.strptime(obj["as_str"], "%Y%m%dT%H:%M:%S.%f")
        return obj

    def save(self):
        for entry in dir(self):
            if entry.startswith("__"):
                continue
            val = getattr(self, entry)

            if isinstance(val, list):
                for listval in val:
                    if isinstance(listval, GitStorage):
                        listval.save()
            elif isinstance(val, GitStorage):
                val.save()

        data = msgpack.packb(self, default=self.encode, use_bin_type=True)
        repo = pygit2.Repository(pygit2.discover_repository(os.getcwd()))
        oid = repo.create_blob(data)

        try:
            ref = repo.lookup_reference(f"refs/renku/{self.schema}")
            commit = ref.peel()
            existing_tree = commit.tree
            existing_commit = [commit.oid]
        except KeyError:
            existing_commit = []
            existing_tree = None

        if existing_tree:
            tree = repo.TreeBuilder(existing_tree)
        else:
            tree = repo.TreeBuilder()

        tree.insert(quote(self.id_, safe=""), oid, pygit2.GIT_FILEMODE_BLOB)
        tree_oid = tree.write()
        sig = pygit2.Signature("Bob", "bob@example.com")
        repo.create_commit(
            f"refs/renku/{self.schema}",
            sig,
            sig,
            f"storing {self.id_}",
            tree_oid,
            existing_commit,
        )

    @classmethod
    def load(cls, id_):
        repo = pygit2.Repository(pygit2.discover_repository(os.getcwd()))

        try:
            ref = repo.lookup_reference(f"refs/renku/{cls.schema}")
        except KeyError:
            raise ValueError(f"No data found for {cls.schema}")

        commit = ref.peel()

        entry = commit.tree[quote(id_, safe="")]
        return msgpack.unpackb(entry.data, object_hook=cls.decode, raw=False)

    @classmethod
    def load_all(cls):
        repo = pygit2.Repository(pygit2.discover_repository(os.getcwd()))

        try:
            ref = repo.lookup_reference(f"refs/renku/{cls.schema}")
        except KeyError:
            raise ValueError(f"No data found for {cls.schema}")

        commit = ref.peel()

        return [cls.load(unquote(e.name)) for e in commit.tree]


class Activity(GitStorage):
    schema = "activity"

    def __init__(
        self,
        id_,
        ended_at_time,
        generated,
        invalidated,
        order,
        qualified_usage,
        started_at_time,
    ):
        self.ended_at_time = ended_at_time
        self.generated = generated
        self.id_ = id_ or f"http://example.com/activities/{uuid.uuid4()}"
        self.invalidated = invalidated
        self.order = order
        self.qualified_usage = qualified_usage
        self.started_at_time = started_at_time

    def encode(self, obj):
        if obj == self:
            return {
                "id": self.id_,
                "generations": [g.id_ for g in self.generated],
                "usages": [u.id_ for u in self.qualified_usage],
                "invalidations": [i.id_ for i in self.invalidated],
                "started_at": self.started_at_time,
                "ended_at": self.ended_at_time,
                "order": self.order,
                "__schema__": self.schema,
            }
        return super().encode(obj)

    @classmethod
    def decode(cls, obj):
        if "__schema__" in obj and obj["__schema__"] == cls.schema:
            return Activity(
                obj["id"],
                obj["ended_at"],
                [Generation.load(g) for g in obj["generations"]],
                obj["invalidations"],
                obj["order"],
                [Usage.load(g) for g in obj["usages"]],
                obj["started_at"],
            )

        return super(Activity, cls).decode(obj)


class Generation(GitStorage):
    schema = "generation"

    def __init__(self, activity_id, entity, role, id_):
        self.id_ = id_ or f"{activity_id}/generation/f{entity.id_}"
        self.entity = entity
        self.role = role

    def encode(self, obj):
        if obj == self:
            return {
                "id": self.id_,
                "role": self.role,
                "entity": self.entity.id_,
                "__schema__": self.schema,
            }
        return super().encode(obj)

    @classmethod
    def decode(cls, obj):
        if "__schema__" in obj and obj["__schema__"] == cls.schema:
            return Generation(None, Entity.load(obj["entity"]), obj["role"], obj["id"])

        return super(Generation, cls).decode(obj)


class Usage(GitStorage):
    schema = "usage"

    def __init__(self, activity_id, entity, role, id_):
        self.id_ = id_ or f"{activity_id}/usage/f{entity.id_}"
        self.entity = entity
        self.role = role

    def encode(self, obj):
        if obj == self:
            return {
                "id": self.id_,
                "role": self.role,
                "entity": self.entity.id_,
                "__schema__": self.schema,
            }
        return super().encode(obj)

    @classmethod
    def decode(cls, obj):
        if "__schema__" in obj and obj["__schema__"] == cls.schema:
            return Usage(None, Entity.load(obj["entity"]), obj["role"], obj["id"])

        return super(Usage, cls).decode(obj)


class Entity(GitStorage):
    schema = "entity"

    def __init__(self, path, checksum):
        self.id_ = f"http://example.com/entities/{checksum}"
        self.path = path
        self.checksum = checksum

    def encode(self, obj):
        if obj == self:
            return {
                "id": self.id_,
                "path": self.path,
                "checksum": self.checksum,
                "__schema__": self.schema,
            }
        return super().encode(obj)

    @classmethod
    def decode(cls, obj):
        if "__schema__" in obj and obj["__schema__"] == cls.schema:
            return Entity(obj["path"], obj["checksum"])

        return super(Entity, cls).decode(obj)


if __name__ == "__main__":
    for _ in range(1000):
        entity = Entity(
            "/path/to/my/{}".format(
                "".join(
                    random.choice(string.ascii_uppercase + string.digits)
                    for _ in range(20)
                )
            ),
            random.randint(1, 999999999),
        )
        entity2 = Entity(
            "/path/to/my/{}".format(
                "".join(
                    random.choice(string.ascii_uppercase + string.digits)
                    for _ in range(20)
                )
            ),
            random.randint(1, 999999999),
        )

        activity_id = f"http://example.com/activities/{uuid.uuid4()}"

        generation = Generation(activity_id, entity2, "some role", None)
        usage = Usage(activity_id, entity, "some other role", None)

        activity = Activity(
            activity_id,
            datetime.datetime.utcnow(),
            [generation],
            [],
            1,
            [usage],
            datetime.datetime.utcnow(),
        )

        activity.save()

    print("Generated 1000 activities")
