hello_world:
	@echo "Hello!"

recreate_venv:
	@echo "Removing and recreating environment"
	@deactivate
	@rm -r venv
	@python -m venv venv
	@echo "Done!"

# . venv/bin/activate for Linux
activate_venv:
	@echo "Activating virtual environment"
	@. venv/Scripts/activate 
	@echo "Done!"

# needs activate venv first:
install_requirements:
	@echo "Installing dependencies..."
	@pip install pip-tools
	@pip-compile requirements.in && pip install -r requirements.txt
	@echo "Done!"

