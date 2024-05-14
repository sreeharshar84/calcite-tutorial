SELECT c_name, o_orderkey, o_orderdate FROM customer
INNER JOIN orders ON c_custkey = o_custkey
WHERE c_custkey = 13
ORDER BY c_name, o_orderkey
