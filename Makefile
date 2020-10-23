check:
	python3 -m flake8 synctool
	python3 -m pylint --rcfile pylintrc --score=n synctool
