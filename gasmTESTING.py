import sys
import gasm

def main():
	t = gasm.gasmfunc("AATGTCC","ATCTCGC", 3, 3, 4, 5, 1)

	if not isinstance(t, tuple):
		print("DISASTER!")
		sys.exit()
	print("Great success!")

if __name__ == "__main__":
	main()
