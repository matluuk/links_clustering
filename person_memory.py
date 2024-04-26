import json
import pysqlite3

from links_cluster import Subcluster, Cluster

class PersonMemory:
    
    def __init__(self, logger, db_file):
        self.db_file = db_file
        self.logger = logger

        self.logger.info(f"pysqlite3 version: {pysqlite3.version}")

        self.logger.info(f"Face database parameters:\n" +
                         f"db_file={self.db_file}\n")

        self.conn = self.__create_connection(self.db_file)
        # db_engine = create_engine(f"sqlite:///{db_file}")
        self.initialize_db()
        self.logger.info("Face database initialized!")

    def __create_connection(self, db_file):
        conn = None
        try:
            conn = pysqlite3.connect(db_file)  # Creates a SQLite database in the current directory
            self.logger.info(f"Connected to database with sqlite version {pysqlite3.version}")
        except pysqlite3.Error as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
        return conn

    def initialize_db(self):
        c = self.conn.cursor()

        # Check if the 'clusters' table exists
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='clusters'")
        if not c.fetchone():
            self.logger.info("Creating 'clusters' table")
            self.__create_clusters_table()
    
    def __create_clusters_table(self):
        try:
            c = self.conn.cursor()
            c.execute("""
                      CREATE TABLE clusters(
                      id VARCHAR(36) NOT NULL,
                      subclusters JSONB DEFAULT('[]'),
                      PRIMARY KEY(id))
                      """)
        except pysqlite3.Error as e:
            self.logger.error(f"Failed to create 'clusters' table: {e}")
            raise

    def add_cluster(self, cluster: Cluster):

        cluster_dict = cluster.as_dict()
        subclusters_json = json.dumps(cluster_dict["subclusters"])
        cluster_dict["subclusters"] = subclusters_json
        self.logger.info(f"Adding cluster: \n{cluster_dict}")
        try:
            with self.conn:
                
                # Datatypes supported by SQLite
                self.conn.execute("""
                    INSERT INTO clusters 
                    (id, subclusters)
                    VALUES
                    (:id, json(:subclusters))
                """, cluster_dict)

            self.logger.info(f"Added new cluster to database: id={cluster.id}")
        except pysqlite3.Error as e:
            self.logger.error(f"Failed to add new subcluster: {e}")
            raise

    def get_cluster(self, id) -> dict:
        try:
            c = self.conn.cursor()
            c.execute("SELECT * FROM clusters WHERE id = ?", (id,))
            columns = [column[0] for column in c.description]
            data = c.fetchone()
            if data is None:
                self.logger.info(f"No cluster found for id: {id}")
                return None
            self.logger.info(f"Get cluster for id: {id}")

            self.logger.info(f"Data={data}")

            # create dict
            cluster_dict = dict(zip(columns, data))

            subclusters_json = cluster_dict["subclusters"]

            cluster_dict["subclusters"] = json.loads(subclusters_json)
            return cluster_dict
        except pysqlite3.Error as e:
            self.logger.error(f"Failed to get cluster for id {id}: {e}")
            raise

    def update_cluster(self, cluster):
        cluster_dict = cluster.as_dict()
        subclusters_json = json.dumps(cluster_dict["subclusters"])
        cluster_dict["subclusters"] = subclusters_json
        try:
            with self.conn:
                
                # Datatypes supported by SQLite
                self.conn.execute("""
                    UPDATE clusters SET 
                    subclusters = (json(:subclusters))
                    Where
                    id = :id 
                """, cluster_dict)

            self.logger.info(f"Updated cluster id={cluster.id}")
        except pysqlite3.Error as e:
            self.logger.error(f"Failed to update cluster={cluster}: {e}")
            raise

    def delete_cluster(self, cluster_id):
        self.logger.info(f"Removing cluster with id: {cluster_id}")
        try:
            with self.conn:
                # Datatypes supported by SQLite
                self.conn.execute("""
                    DELETE FROM clusters
                    WHERE
                    id = ?
                """, (cluster_id,))

            self.logger.info(f"Deleted cluster id={cluster_id}")
        except pysqlite3.Error as e:
            self.logger.error(f"Failed to deleted cluster id={cluster_id}: {e}")
            raise

    def add_subcluster(self, cluster_id, subcluster):
        subcluster_dict = subcluster.as_dict()
        subclusters_json = json.dumps(subcluster_dict)
        self.logger.info(f"Adding subcluster ({subcluster.id}) to cluster id={cluster_id}: \n{subcluster_dict}")
        try:
            with self.conn:
                
                # Datatypes supported by SQLite
                self.conn.execute("""
                    UPDATE clusters SET 
                    subclusters = (select json_insert(clusters.subclusters, '$[#]', jsonb(?)) from clusters)
                    WHERE
                    id = ?
                """, (subclusters_json, cluster_id))

            self.logger.info(f"Added subcluster to cluster: id={cluster_id}")
        except pysqlite3.Error as e:
            self.logger.error(f"Failed to add new subcluster to cluster: id={cluster_id}: {e}")
            raise

    def get_subcluster(self, cluster_id, subcluster_index):
        self.logger.info(f"Getting subcluster ({subcluster_index}) of cluster id={cluster_id}.")
        try:
            with self.conn:
                
                subcluster_index_str = f"$[{subcluster_index}]"
                c = self.conn.cursor()
                c.execute("""
                    SELECT value
                    FROM
                        clusters as C,
                        json_tree( C.subclusters, ?) AS T
                    WHERE C.id= ?
                """, (subcluster_index_str, cluster_id))
            data = c.fetchone()
            if data is None:
                self.logger.info(f"No subcluster found with index: {subcluster_index} in cluster {cluster_id}")
                return None
            self.logger.info(f"Get subcluster with index: {subcluster_index} from cluster {cluster_id}")

            self.logger.info(f"Data={data}")

            subcluster_dict = json.loads(data[0])
            return subcluster_dict
        except pysqlite3.Error as e:
            self.logger.error(f"Failed to get subcluster ({subcluster_index}) of cluster id={cluster_id}: {e}")
            raise

    def update_subcluster(self, cluster_id, subcluster, subcluster_index):
        subcluster_dict = subcluster.as_dict()
        subclusters_json = json.dumps(subcluster_dict)
        self.logger.info(f"Updating subcluster ({subcluster.id}) of cluster id={cluster_id}: \n{subcluster_dict}")
        try:
            with self.conn:
                subcluster_index_str = f"$[{subcluster_index}]"
                self.conn.execute("""
                    UPDATE clusters SET 
                    subclusters = (select json_replace(clusters.subclusters, ?, jsonb(?)) from clusters)
                    WHERE
                    id = ?
                """, (subcluster_index_str, subclusters_json, cluster_id))

            self.logger.info(f"Updated subcluster {subcluster_index} in cluster: id={cluster_id}")
        except pysqlite3.Error as e:
            self.logger.error(f"Failed to add new subcluster to cluster: id={cluster_id}: {e}")
            raise

    def delete_subcluster(self, cluster_id, subcluster_index):
        self.logger.info(f"Deleting subcluster ({subcluster_index}) of cluster id={cluster_id}.")
        try:
            with self.conn:
                subcluster_index_str = f"$[{subcluster_index}]"
                self.conn.execute("""
                    UPDATE clusters SET 
                    subclusters = (select json_remove(clusters.subclusters, ?) from clusters)
                    WHERE
                    id = ?
                """, (subcluster_index_str, cluster_id))
            self.logger.info(f"Deleted subcluster {subcluster_index} of cluster: id={cluster_id}")
        except pysqlite3.Error as e:
            self.logger.error(f"Failed to add new subcluster to cluster: id={cluster_id}: {e}")
            raise

    def get_all_clusters(self) -> dict:
        self.logger.info("Retrieving clusters from database")
        try:
            c = self.conn.cursor()
            c.execute("SELECT * FROM clusters")
            columns = [column[0] for column in c.description]
            data = c.fetchall()
            if data is None:
                self.logger.info(f"No clusters in database")
                return None
            self.logger.info(f"Get clusters")

            self.logger.info(f"data={data}")
            self.logger.info(f"columns={columns}")

            cluster_dicts = []

            for row in data:

                # create dict
                cluster_dict = dict(zip(columns, row))
                self.logger.info(f"cluster_dict={cluster_dict}")
                # self.logger.info(f"cluster_dict.id={cluster_dict["id"]}")

                subclusters_json = cluster_dict["subclusters"]
                self.logger.info(f"subclusters_json={subclusters_json}")
                
                cluster_dict["subclusters"] = json.loads(subclusters_json)
                cluster_dicts.append(cluster_dict)
            self.logger.info("Clusters retrieved from database")
            return cluster_dicts
        except pysqlite3.Error as e:
            self.logger.error(f"Failed to get clusters: {e}")
            raise
