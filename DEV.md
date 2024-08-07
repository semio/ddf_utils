# some notes on publishing

- double check the version number in `__init__.py` and `Changelogs.md`
- first do `python setup.py tag` to create a tag
- then `python -m build` to create wheel and tarball in `dist/`
- then `twine upload dist/*` to upload to pypi.
