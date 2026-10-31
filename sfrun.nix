{
  lib,
  buildPythonPackage,
  setuptools,
  jinja2,
  openpyxl,
  sfconn,
  snowflake-snowpark-python,
  yappt,
}:

buildPythonPackage {
  pname = "sfrun";
  version = "0.2.0";
  src = ./.;
  pyproject = true;

  propagatedBuildInputs = [
    jinja2
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
