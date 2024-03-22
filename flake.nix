{
  description = "Snowflake SQL pretty printer";

  inputs = {
    nixpkgs.url   = "github:nixos/nixpkgs/nixos-unstable";
    nix-utils.url = "github:padhia/nix-utils";
    sfconn.url    = "github:padhia/sfconn/next";
    snowflake.url = "github:padhia/snowflake/next";
    yappt.url     = "github:padhia/yappt/next";

    nix-utils.inputs.nixpkgs.follows = "nixpkgs";
    sfconn.inputs.nixpkgs.follows    = "nixpkgs";
    snowflake.inputs.nixpkgs.follows = "nixpkgs";
    yappt.inputs.nixpkgs.follows     = "nixpkgs";
  };

  outputs = { self, nixpkgs, flake-utils, nix-utils, sfconn, snowflake, yappt }:
    nix-utils.lib.mkPyFlake {
      pkgs     = { sqlexp = import ./sqlexp.nix; };
      deps     = [ "sfconn" "snowflake-snowpark-python" "yappt" "openpyxl" ];
      apps     = [ "sqlexp" ];
      pyFlakes = [ sfconn yappt snowflake ];
    };
}
