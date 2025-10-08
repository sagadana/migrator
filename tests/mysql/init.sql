-- Grant replication privileges to the test user
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'test-mysql-user'@'%';
FLUSH PRIVILEGES;