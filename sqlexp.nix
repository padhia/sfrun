{ lib, buildPythonPackage, setuptools, openpyxl, sfconn, snowflake-snowpark-python, yappt }:

buildPythonPackage rec {
  pname = "sqlexp";
  version = "0.1.0";
  src = ./.;
  pyproject = true;

  propagatedBuildInputs = [
    openpyxl
    sfconn
    snowflake-snowpark-python
    yappt
  ];

  nativeBuildInputs = [
    setuptools
  ];

  doCheck = false;

  meta = with lib; {
    homepage = "https://github.com/padhia/sqlexp";
    description = "Batch SQL runner for Snowflake database";
    maintainers = with maintainers; [ padhia ];
  };
}
