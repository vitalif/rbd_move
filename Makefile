all: rbd-move
rbd-move: rbd-move.c
	gcc -g -o rbd-move -lrbd -lrados rbd-move.c
