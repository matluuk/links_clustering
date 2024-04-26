import pytest

import logging
import os
import numpy as np
from pathlib import Path

from person_memory import PersonMemory
from links_cluster import Subcluster, Cluster

logger = logging.getLogger()

class TestPersonMemory:
    @classmethod
    def setup_class(cls):
        logger.info("starting class: {} execution".format(cls.__name__))

    @classmethod
    def teardown_class(cls):
        logger.info("starting class: {} execution".format(cls.__name__))

    def setup_method(self, method):
        logger.info("starting execution of tc: {}".format(method.__name__))

        self.person_memory = self.create_db()

    def teardown_method(self, method):
        logger.info("teardown of tc: {}".format(method.__name__))
        self.delete_db()

    @staticmethod
    def delete_db():
        db_path = ":memory:"
        logger.info(f"Deleting database: '{db_path}'")
        os.remove(db_path)

    @staticmethod
    def create_db():
        db_path = ":memory:"
        person_memory = PersonMemory(logger=logger, db_file=db_path)
        return person_memory

    def test_update_cluster(self):

        subcluster = Subcluster([1.1, 2.2, 3.3, 4.4], True, logger=logger)
        subcluster_2 = Subcluster([1.1, 2.2, 3.3, 4.4], True, logger=logger)

        cluster = Cluster(subcluster, logger=logger)
        logger.info(f"Cluster dict: \n{cluster.as_dict()}")

        self.person_memory.add_cluster(cluster)

        cluster_dict_from_db = self.person_memory.get_cluster(str(cluster.id))

        logger.info(f"Cluster dict from db: \n{cluster_dict_from_db}")

        cluster_from_db = Cluster.from_dict(cluster_dict_from_db, logger)

        logger.info(f"Cluster from db: \n{cluster_from_db}")

        self.verify_two_clusters(cluster, cluster_from_db)

        cluster.add_subcluster(subcluster_2)

        self.person_memory.update_cluster(cluster)

        cluster_dict_from_db = self.person_memory.get_cluster(str(cluster.id))

        logger.info(f"Cluster dict from db: \n{cluster_dict_from_db}")

        cluster_from_db = Cluster.from_dict(cluster_dict_from_db, logger)

        logger.info(f"Cluster from db: \n{cluster_from_db}")

        self.verify_two_clusters(cluster, cluster_from_db)

        logger.info("OK!")

    def test_delete_cluster(self):

        subcluster = Subcluster([1.1, 2.2, 3.3, 4.4], True, logger=logger)

        cluster = Cluster(subcluster, logger=logger)
        logger.info(f"Cluster dict: \n{cluster.as_dict()}")

        self.person_memory.add_cluster(cluster)

        cluster_dict_from_db = self.person_memory.get_cluster(str(cluster.id))

        logger.info(f"Cluster dict from db: \n{cluster_dict_from_db}")

        cluster_from_db = Cluster.from_dict(cluster_dict_from_db, logger)

        logger.info(f"Cluster from db: \n{cluster_from_db}")

        self.verify_two_clusters(cluster, cluster_from_db)

        self.person_memory.delete_cluster(cluster.id)

        cluster_dict_from_db = self.person_memory.get_cluster(str(cluster.id))

        assert cluster_dict_from_db is None

        logger.info("OK!")

    def test_add_and_get_cluster(self):

        subcluster = Subcluster([1.1, 2.2, 3.3, 4.4], True, logger=logger)

        cluster = Cluster(subcluster, logger=logger)
        logger.info(f"Cluster dict: \n{cluster.as_dict()}")

        self.person_memory.add_cluster(cluster)

        cluster_dict_from_db = self.person_memory.get_cluster(str(cluster.id))

        logger.info(f"Cluster dict from db: \n{cluster_dict_from_db}")

        cluster_from_db = Cluster.from_dict(cluster_dict_from_db, logger)

        logger.info(f"Cluster from db: \n{cluster_from_db}")

        self.verify_two_clusters(cluster, cluster_from_db)

        logger.info("OK!")

    def test_cluster_three_subclusters(self):

        subcluster = Subcluster([1.1, 2.1, 3.1, 4.1], True, logger=logger)
        subcluster_2 = Subcluster([1.2, 2.2, 3.2, 4.2], True, logger=logger)
        subcluster_3 = Subcluster([1.3, 2.3, 3.3, 4.3], True, logger=logger)

        cluster = Cluster(subcluster, logger=logger)
        cluster.add_subcluster(subcluster_2)
        cluster.add_subcluster(subcluster_3)
        logger.info(f"Cluster dict: \n{cluster.as_dict()}")

        self.person_memory.add_cluster(cluster)

        cluster_dict_from_db = self.person_memory.get_cluster(str(cluster.id))

        logger.info(f"Cluster dict from db: \n{cluster_dict_from_db}")

        cluster_from_db = Cluster.from_dict(cluster_dict_from_db, logger)

        logger.info(f"Cluster from db: \n{cluster_from_db}")

        self.verify_two_clusters(cluster, cluster_from_db)

    def test_get_all_clusters(self):

        subcluster = Subcluster([1.1, 2.1, 3.1, 4.1], True, logger=logger)
        subcluster_2 = Subcluster([1.2, 2.2, 3.2, 4.2], True, logger=logger)

        cluster = Cluster(subcluster, logger=logger)
        cluster_2 = Cluster(subcluster_2, logger=logger)

        clusters = [cluster, cluster_2]

        self.person_memory.add_cluster(cluster)
        self.person_memory.add_cluster(cluster_2)

        clusters_dict_from_db = self.person_memory.get_all_clusters()

        clusters_from_db = [Cluster.from_dict(cluster_dict_from_db) for cluster_dict_from_db in clusters_dict_from_db]

        logger.info(f"clusters={clusters}")
        logger.info(f"clusters_from_db={clusters_from_db}")

        assert len(clusters) == len(clusters_from_db)
        for i in range(len(clusters)):
            self.verify_two_clusters(clusters[i], clusters_from_db[i])

        logger.info("OK!")

    def test_add_subcluster(self):

        subcluster = Subcluster([1.1, 2.2, 3.3, 4.4], True, logger=logger)
        subcluster_2 = Subcluster([1.2, 2.2, 3.2, 4.2], True, logger=logger)

        cluster = Cluster(subcluster, logger=logger)
        logger.info(f"Cluster dict: \n{cluster.as_dict()}")

        self.person_memory.add_cluster(cluster)

        cluster_dict_from_db = self.person_memory.get_cluster(str(cluster.id))

        logger.info(f"Cluster dict from db: \n{cluster_dict_from_db}")

        cluster_from_db = Cluster.from_dict(cluster_dict_from_db, logger)

        logger.info(f"Cluster from db: \n{cluster_from_db}")

        self.verify_two_clusters(cluster, cluster_from_db)
        
        self.person_memory.add_subcluster(cluster_id=cluster.id, subcluster=subcluster_2)

        cluster.add_subcluster(subcluster_2)

        cluster_dict_from_db = self.person_memory.get_cluster(cluster.id)

        logger.info(f"cluster_dict_from_db-type={type(cluster_dict_from_db)}")
        logger.info(f"cluster_dict_from_db['subclusters']-type={type(cluster_dict_from_db['subclusters'])}")
        logger.info(f"cluster_dict_from_db['subclusters'][0]-type={type(cluster_dict_from_db['subclusters'][0])}")
        logger.info(f"cluster_dict_from_db['subclusters'][0]['id']-type={type(cluster_dict_from_db['subclusters'][0]['id'])}")

        logger.info(f"Cluster dict from db: \n{cluster_dict_from_db}")

        cluster_from_db = Cluster.from_dict(cluster_dict_from_db, logger)

        logger.info(f"Cluster from db: \n{cluster_from_db}")

        self.verify_two_clusters(cluster, cluster_from_db)

        logger.info("OK!")

    def test_get_subcluster(self):
        subcluster = Subcluster([1.1, 2.2, 3.3, 4.4], True, logger=logger)
        cluster = Cluster(subcluster, logger=logger)
        logger.info(f"Cluster dict: \n{cluster.as_dict()}")

        self.person_memory.add_cluster(cluster)

        subcluster_dict_from_db = self.person_memory.get_subcluster(cluster.id, 0)
        logger.info(f"subcluster_dict={subcluster_dict_from_db}")
        subcluster_from_db = Subcluster.from_dict(subcluster_dict_from_db)
        self.verify_two_subclusters(subcluster, subcluster_from_db)

        subcluster_dict_from_db = self.person_memory.get_subcluster(cluster.id, 1)
        assert subcluster_dict_from_db is None

    def test_update_subcluster(self):

        subcluster = Subcluster([1.1, 2.2, 3.3, 4.4], True, logger=logger)
        # subcluster = Subcluster(np.array([1.1, 2.2, 3.3, 4.4]), True, logger=logger)
        cluster = Cluster(subcluster, logger=logger)
        logger.info(f"Cluster dict: \n{cluster.as_dict()}")

        self.person_memory.add_cluster(cluster)

        cluster_dict_from_db = self.person_memory.get_cluster(str(cluster.id))
        logger.info(f"Cluster dict from db: \n{cluster_dict_from_db}")
        cluster_from_db = Cluster.from_dict(cluster_dict_from_db, logger)
        logger.info(f"Cluster from db: \n{cluster_from_db}")
        self.verify_two_clusters(cluster, cluster_from_db)
        
        subcluster.add([1.2, 2.2, 3.2, 4.2])

        self.person_memory.update_subcluster(cluster_id=cluster.id, subcluster=cluster.subclusters[0], subcluster_index=0)

        cluster_dict_from_db = self.person_memory.get_cluster(cluster.id)
        logger.info(f"Cluster dict from db: \n{cluster_dict_from_db}")
        cluster_from_db = Cluster.from_dict(cluster_dict_from_db, logger)
        logger.info(f"Cluster from db: \n{cluster_from_db}")
        self.verify_two_clusters(cluster, cluster_from_db)

        logger.info("OK!")

    def test_delete_subcluster(self):

        subcluster = Subcluster([1.1, 2.1, 3.1, 4.1], True, logger=logger)
        subcluster_2 = Subcluster([1.2, 2.2, 3.2, 4.2], True, logger=logger)

        cluster = Cluster(subcluster, logger=logger)
        logger.info(f"Cluster dict: \n{cluster.as_dict()}")
        cluster.add_subcluster(subcluster_2)
        self.person_memory.add_cluster(cluster)
    
        cluster_dict_from_db = self.person_memory.get_cluster(str(cluster.id))
        logger.info(f"Cluster dict from db: \n{cluster_dict_from_db}")
        cluster_from_db = Cluster.from_dict(cluster_dict_from_db, logger)
        logger.info(f"Cluster from db: \n{cluster_from_db}")
        self.verify_two_clusters(cluster, cluster_from_db)

        # Delete subcluster with id 0
        self.person_memory.delete_subcluster(cluster_id=cluster.id, subcluster_index=0)

        cluster_dict_from_db = self.person_memory.get_cluster(cluster.id)
        logger.info(f"Cluster dict from db: \n{cluster_dict_from_db}")
        cluster_from_db = Cluster.from_dict(cluster_dict_from_db, logger)
        logger.info(f"Cluster from db: \n{cluster_from_db}")

        # Verify that 
        assert len(cluster_from_db.subclusters) == 1
        self.verify_two_subclusters(cluster.subclusters[1], cluster_from_db.subclusters[0])

        logger.info("OK!")

    def convert_subcluster_dicts(subcluster_dicts):
        subclusters = []
        for subcluster_dict in subcluster_dicts:
            subclusters.append(
                Subcluster.from_dict(subcluster_dict, logger)
            )

    def update_subcluster_connections(subclusters):
        for subcluster in subclusters:
            pass
    
    def verify_two_clusters(self, cluster_1: Cluster, cluster_2: Cluster):
        logger.info(f"Verifying clusters: \n cluster_1={cluster_1.as_dict()}\n cluster_2={cluster_2.as_dict()}")
        assert cluster_1.id == cluster_2.id
        assert len(cluster_1.subclusters) == len(cluster_2.subclusters)
        for i in range(len(cluster_1.subclusters)):
            self.verify_two_subclusters(cluster_1.subclusters[i], cluster_2.subclusters[i])

    @staticmethod
    def verify_two_subclusters(subcluster_1: Subcluster, subcluster_2: Subcluster):
        assert subcluster_1.id == subcluster_2.id
        assert subcluster_1.vectors == subcluster_2.vectors
        assert subcluster_1.centroid == subcluster_2.centroid
        assert subcluster_1.vector_count == subcluster_2.vector_count
        assert subcluster_1.store_vectors == subcluster_2.store_vectors
        # assert subcluster_1.connected_subclusters == subcluster_2.connected_subclusters
        assert subcluster_1.last_seen == subcluster_2.last_seen
        assert subcluster_1.current_conversation["start_time"] == subcluster_2.current_conversation["start_time"]
        assert subcluster_1.current_conversation["end_time"] == subcluster_2.current_conversation["end_time"]
        assert subcluster_1.current_conversation["duration"] == subcluster_2.current_conversation["duration"]

        assert len(subcluster_1.conversations) == len(subcluster_2.conversations)
        for i in range(len(subcluster_1.conversations)):
            assert subcluster_1.conversations[i]["start_time"] == subcluster_2.conversations[i]["start_time"]
            assert subcluster_1.conversations[i]["end_time"] == subcluster_2.conversations[i]["end_time"]
            assert subcluster_1.conversations[i]["duration"] == subcluster_2.conversations[i]["duration"]
        assert subcluster_1.total_time_on_camera == subcluster_2.total_time_on_camera
