default: binaries

python:
	cd python/wiggledb; make

clean:
	rm bin/*
	rm lib/*
