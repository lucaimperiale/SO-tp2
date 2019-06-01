
#!/usr/bin/env python3
import os
import subprocess
import sys
 
def main():
	n = int(sys.argv[1])
	with open("log/diff.txt","w") as Salida:
		os.system("mpirun -np " + str(n) + " ./blockchain" )
		i = 0
		for i in range(n):
			for j in range(i,n):
				if i != j:
					process = subprocess.Popen(["diff","log/" + str(i) + ".txt","log/" + str(j) + ".txt"],stdout=subprocess.PIPE)
					salida = process.communicate()
					Salida.write(str(i) + " - " + str(j) +":" + str(salida) + "\n")
       
	Salida.close
 
 
main()