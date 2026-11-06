#!/bin/bash
# gasi samo kontejnere i volume-e ovog projekta (ne dira ostale Docker projekte)
docker compose -f docker-compose-hue.yml down -v --remove-orphans
