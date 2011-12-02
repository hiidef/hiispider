from MySQLdb.cursors import DictCursor
from twisted.enterprise import adbapi


class MySQLMixin(object):

    def setupMySQL(self, config):
        self.mysql = adbapi.ConnectionPool(
            "MySQLdb",
            db=config["mysql_database"],
            port=config.get("mysql_port", 3306),
            user=config["mysql_username"],
            passwd=config["mysql_password"],
            host=config["mysql_host"],
            cp_reconnect=True,
            cursorclass=DictCursor)
