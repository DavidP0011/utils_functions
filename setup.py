from setuptools import setup, find_packages
import pathlib


here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text(encoding="utf-8") if (here / "README.md").exists() else ""

setup(
    name='dpm_functions',
    version='0.1',
    description='Conjunto de funciones para procesos ETL',
    url='https://github.com/DavidP0011/etl_functions',
    author='David Plaza', 
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    python_requires='>=3.7, <4',
    install_requires=[
        "google-cloud-secretmanager>=2.7.0",  # Para inicialización y autenticación en GCP.
        "google-auth>=2.0.0",                # Utilizado en autenticación con GCP.
        "PyPDF2>=3.0.0",                    # Para el manejo y procesamiento de PDFs.
        "pandas>=1.3.0"                     # Para manipulación y análisis de tablas.
    ]


    entry_points={
        # Scripts ejecutables desde la línea de comando
        # 'console_scripts': [
        #     'nombre_comando=dpm_functions.modulo:funcion_principal',
        # ],
    },
)
