{
  description = "Snwoflake SQL runner";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    nix-utils.url = "github:padhia/nix-utils/next";
    nix-utils.inputs.nixpkgs.follows = "nixpkgs";

    snowflake.url = "github:padhia/snowflake/next";
    snowflake.inputs = {
      nixpkgs.follows = "nixpkgs";
      flake-utils.follows = "flake-utils";
    };

    sfconn.url = "github:padhia/sfconn/next";
    sfconn.inputs = {
      nixpkgs.follows = "nixpkgs";
      snowflake.follows = "snowflake";
      flake-utils.follows = "flake-utils";
    };

    yappt.url = "github:padhia/yappt/next";
    yappt.inputs = {
      nixpkgs.follows = "nixpkgs";
      flake-utils.follows = "flake-utils";
    };
  };

  outputs = { self, nixpkgs, flake-utils, nix-utils, sfconn, snowflake, yappt }:
  let
    inherit (nix-utils.lib) pyDevShell extendPyPkgsWith mkApps;

    overlays.default = final: prev:
      extendPyPkgsWith prev { sfrun = ./sfrun.nix; } // {sfrun = final.python311Packages.sfrun; };

    eachSystem = system:
    let
      pkgs = import nixpkgs {
        inherit system;
        config.allowUnfree = true;
        overlays = [
          yappt.overlays.default
          sfconn.overlays.default
          snowflake.overlays.default
          self.overlays.default
        ];
      };

      devShells.default = pyDevShell {
        inherit pkgs;
        name = "sfrun";
        extra = [
          "openpyxl"
          "snowflake-snowpark-python"
          "sfconn"
          "yappt"
        ];
        pyVer = "311";
      };

      packages.default = pkgs.sfrun;

      apps = mkApps {
        inherit pkgs;
        pkg = packages.default;
        cmds = [ "sfrun" "sfrunb" ];
      };

    in {
      inherit devShells packages apps;
    };
  in {
    inherit overlays;
    inherit (flake-utils.lib.eachDefaultSystem eachSystem) devShells packages apps;
  };
}
