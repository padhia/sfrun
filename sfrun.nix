{
  lib,
  buildPythonPackage,
  setuptools,
  openpyxl,
  sfconn,
  snowflake-snowpark-python,
  yappt
}:

buildPythonPackage {
  pname = "sfrun";
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
    homepage = "https://github.com/padhia/sfrun";
    description = "Snwoflake SQL runner";
    maintainers = with maintainers; [ padhia ];
  };
}
