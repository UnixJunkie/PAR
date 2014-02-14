PACKAGE = "par"
NAME = "PAR"
DESCRIPTION = "PAR: a PARallel and distributed job crusher"
AUTHOR = "Francois Berenger"
AUTHOR_EMAIL = "berenger@riken.jp"
URL = "https://github.com/UnixJunkie/PAR"
VERSION = __import__(PACKAGE).__version__

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=read("README.rst"),
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    license="GPL",
    url=URL,
    packages=find_packages(exclude=["tests.*", "tests"]),
    package_data=find_package_data(
			PACKAGE,
			only_in_packages=False
	  ),
    classifiers=[
        "Development Status :: 6 - Mature",
        "Environment :: Console",
        "Intended Audience :: End Users/Desktop",
        "Intended Audience :: Science/Research",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: POSIX",
        "Programming Language :: Python",
    ],
    install_requires=[
        'pyro',
    ],
    zip_safe=False,
)
