# Nightingale

Nightingale is a Python package designed to transform flat data from an SQLite database into the Open Contracting Data Standard (OCDS) format. It utilizes a configuration-driven approach to map data fields and support customizable transformations.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Features

- Load data from SQLite databases and map data fields to the OCDS format based on configuration
- Optionally package transformed data into OCDS release packages

## Installation

Please refer to the [Installation Guide](docs/installation.rst).

## Usage

Please refer to the [Tutorial](docs/tutorial.rst) or read [Examples](docs/examples.rst)

## Documentation

Comprehensive documentation for Nightingale, including an overview, installation guide, tutorial, examples, API reference, CLI reference, and configuration reference, is available in the `docs` directory.

To generate the HTML documentation:

1. Navigate to the `docs` directory.
2. Run the `sphinx-build` command:

   ```sh
   sphinx-build -b html . _build
   ```

3. Open the generated HTML files in your browser.

For detailed documentation, please refer to the [docs](docs/index.rst).

## Contributing

We welcome contributions!

## License

Nightingale is open-source software licensed under the BSD 3-Clause License. See the [LICENSE](LICENSE) file for more information.
