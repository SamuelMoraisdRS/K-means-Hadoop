"""
Input: cada linha com um ponto x (latitude e longitude)
Output: Chave (centroide mais proximo) e
 valor (proprio ponto #!/usr/bin/python3

"""

import sys
import os

str_pair_to_float_pair = lambda p: [float(p[0]), float(p[1])]

# O MapReduce sera realizado multiplas vezes, pegando os resultados do HDFS
iteration = int(sys.argv[1])
# Lista de centroides da iteracao atual
centroids = os.popen(f"/opt/hadoop/bin/hdfs dfs -cat /iterations/{iteration}/part-*").read().split()

# Converte os centroides
centroids = [str_pair_to_float_pair(c.split(",")) for c in centroids]

# Para cada linha do arquivo de tweets
for line in sys.stdin:
    pass