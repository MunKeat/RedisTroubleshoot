.PHONY: base clean dev test

base:
		docker build -t redis-base docker/base

dev:	base
		docker-compose up -d --build

clean:
		docker-compose stop
		docker-compose rm -f
