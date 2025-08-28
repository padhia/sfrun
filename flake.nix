{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    snowflake.url = "github:padhia/snowflake/next";
    sfconn.url = "github:padhia/sfconn/next";
    yappt.url = "github:padhia/yappt/next";

    snowflake.inputs.nixpkgs.follows = "nixpkgs";
    sfconn.inputs.nixpkgs.follows = "nixpkgs";
    yappt.inputs.nixpkgs.follows = "nixpkgs";

    snowflake.inputs.flake-utils.follows = "flake-utils";
    sfconn.inputs.flake-utils.follows = "flake-utils";
    yappt.inputs.flake-utils.follows = "flake-utils";

    sfconn.inputs.snowflake.follows = "snowflake";
  };

  outputs = { self, nixpkgs, flake-utils, sfconn, snowflake, yappt }:
  let
    inherit (nixpkgs.lib) composeManyExtensions;

    overlays.default =
    let
      pkgOverlay = final: prev: {
        sfrun = final.python313Packages.sfrun;

        pythonPackagesExtensions = prev.pythonPackagesExtensions ++ [
          (py-final: py-prev: {
            sfrun = py-final.callPackage ./sfrun.nix {};
          })
        ];
      };
    in composeManyExtensions [
      yappt.overlays.default
      sfconn.overlays.default
      snowflake.overlays.default
      pkgOverlay
    ];

    eachSystem = system:
    let
      pkgs = import nixpkgs {
        inherit system;
        config.allowUnfree = true;
        overlays = [ self.overlays.default ];
      };

      pyPkgs = pkgs.python312Packages;

      devShells.default = pkgs.mkShell {
        name = "sfrun";
        venvDir = "./.venv";
        buildInputs = [
          pkgs.ruff
          pkgs.uv
          pyPkgs.python
          pyPkgs.venvShellHook
          pyPkgs.pytest
          pyPkgs.yappt
          pyPkgs.sfconn
          pyPkgs.snowflake-snowpark-python
        ];
      };

      packages.default = pkgs.sfrun;

      apps.default = {
        type = "app";
        program = "${packages.default}/bin/sfrun";
      };
    in { inherit devShells packages apps; };

  in {
    inherit overlays;
    inherit (flake-utils.lib.eachDefaultSystem eachSystem) devShells packages apps;
  };
}
