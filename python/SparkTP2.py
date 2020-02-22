import sys
import pyspark
import petasky_schema

compte = "p1419771"

def zone(minRA, maxRA, minDecl, maxDecl):
    return dict(minRA = minRA, maxRA = maxRA, minDecl = minDecl, maxDecl = maxDecl)

def zone_englobante(a, b):
    return dict(minRA = min(a['minRA'], b['minRA']), 
                maxRA = min(a['maxRA'], b['maxRA']),
                minDecl = min(a['minDecl'], b['minDecl']),
                maxDecl = min(a['maxDecl'], b['maxDecl'])
                )

def lineToStrings(line):
    return list(map(lambda c: c.strip(), line.split(",")))

def createTuple(r):
    ra = float(r[petasky_schema.s_ra()])
    decl = float(r[petasky_schema.s_decl()])
    return (0, ra, ra, decl, decl, 1)

def createTuple2(r):
    objId = r[petasky_schema.s_objectId()]
    #srcId = r[petasky_schema.s_sourceId()]
    ra = float(r[petasky_schema.s_ra()])
    decl = float(r[petasky_schema.s_decl()])
    return (objId, ra, ra, decl, decl)

def aggTuple(t1, t2):
    return (0, min(t1[1], t2[1]), max(t1[2], t2[2]),
            min(t1[3], t2[3]), max(t1[4], t2[4]), t1[5]+t2[5])

def dim_calculation(inputDir, src):
    rows = src.textFile(inputDir).map(lineToStrings)
    rows = rows.filter(lambda r: r[petasky_schema.s_objectId()] != "NULL")
    cols = rows.map(createTuple)
    key = cols.map(lambda t: (t[0], t))
    return key.reduceByKey(aggTuple).map(lambda t: t[1])

def search_zone(r):
    if r[1] < r[5].sep_RA[0]:
        if r[3] < r[5].sep_Decl[0]:
            return (r[0], "00")
        else:
            return (r[0], "01")
    else:
        if r[3] < r[5].sep_Decl[0]:
            return (r[0], "10")
        else:
            return (r[0], "11")

def separate(inputDir, largi, src):
    rows = src.textFile(inputDir).map(lineToStrings)
    rows = rows.filter(lambda r: r[petasky_schema.s_objectId()] != "NULL")
    cols = rows.map(createTuple2)
    info = cols.map(lambda t: (t[0], t[1], t[2], t[3], t[4], lagri))
    return info.map(search_zone, info)

class Grille:
    def __init__(self, _zone):
        self.nbGrille = 4
        self.sep_RA = [(_zone[1] + _zone[2]) / 2]
        self.sep_Decl = [(_zone[3] + _zone[4]) / 2]

if __name__ == "__main__":
    args = sys.args
    if len(args) > 2 and "-h" not in args and "--help" not in args:
        src = pyspark.SparkContext(appName = "SparkTP2-" + compte)
        result = dim_calculation(args[1], src)
	
	z = result.collect()[0]
	print(z)
	print("===========================================================")
	myGrille = Grille(z)
	resultat = separate(args[1], myGrille, src)
	resultat.map(lambda t: ",".join(map(str, t))).saveAsTextFile(args[2])
    else:
        print("Usage: spark-submit --py-files petasky_schema.py "
              + "--files Source.sql,Object.sql "
              + args[0]
              + " hdfs:///user/"
              + compte
              + "/repertoire-donnees"
              + " hdfs:///user/"
              + compte
              + "/repertoire-resultat"
              )

