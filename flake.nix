{
  description = "Snowflake SQL pretty printer";

  inputs = {
    nixpkgs.url     = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    nixlib.url      = "github:padhia/nixlib";
    yappt.url       = "github:padhia/yappt/next";
    sfconn.url      = "github:padhia/sfconn/next";

    nixlib.flake = false;

    sfconn.inputs.nixpkgs.follows    = "nixpkgs";
    yappt.inputs.nixpkgs.follows     = "nixpkgs";
    yappt.inputs.flake-utils.follows = "flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, nixlib, sfconn, yappt }:
    import "${nixlib}/mkPyFlake.nix" { inherit nixpkgs flake-utils; } {
      name     = "sqlexp";
      builder  = import ./sqlexp.nix;
      deps     = ["sfconn" "yappt" "openpyxl"];
      apps     = ["sqlexp"];
      pyFlakes = [sfconn yappt];
    };
}
