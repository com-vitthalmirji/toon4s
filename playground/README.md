# toon4s Web Playground

An interactive web-based playground for the [toon4s](https://github.com/vim89/toon4s) library, powered by Scala.js.

## Overview

The toon4s playground provides a user-friendly interface to:

- Convert JSON to TOON format (and vice versa)
- Experiment with different delimiters (comma, tab, pipe)
- Adjust indentation settings
- View real-time token savings statistics
- Try pre-loaded examples to understand TOON format benefits

## Features

✨ **Interactive Conversion**: Convert between JSON and TOON formats in real-time

📊 **Token Statistics**: See character count and savings percentage

🎯 **Multiple Delimiters**: Choose between comma, tab, or pipe delimiters

📖 **Example Data**: Pre-loaded examples demonstrating common use cases

🎨 **Modern UI**: Beautiful, responsive design with gradient backgrounds

⚡ **Pure Scala.js**: No JavaScript frameworks needed - 100% Scala code

## Building the Playground

### Prerequisites

- [sbt](https://www.scala-sbt.org/) 1.9.0 or higher
- JDK 11 or higher

### Quick Start

1. **Compile the Scala.js code**:
   ```bash
   sbt playground/fastLinkJS
   ```

2. **Open in browser**:
   Navigate to:
   ```
   playground/src/main/resources/index.html
   ```

   The HTML file references the compiled JavaScript at:
   ```
   ../../../target/scala-js/toon4s-playground-fastopt.js
   ```

### Development Mode

For development with automatic recompilation on file changes:

```bash
sbt ~playground/fastLinkJS
```

This watches for changes and recompiles automatically. Just refresh your browser to see updates.

### Production Build

For optimized production builds:

```bash
sbt playground/fullLinkJS
```

Then update the script tag in `index.html` to use:
```html
<script src="../../../target/scala-js-opt/toon4s-playground-opt.js"></script>
```

## Project Structure

```
playground/
├── src/
│   └── main/
│       ├── scala/
│       │   └── io/toonformat/toon4s/playground/
│       │       └── PlaygroundApp.scala         # Main Scala.js application
│       └── resources/
│           ├── index.html                      # Main HTML page
│           └── styles.css                      # Stylesheet
├── target/
│   ├── scala-js/                               # FastOpt output
│   └── scala-js-opt/                           # FullOpt output
└── README.md
```

## Architecture

The playground is built with:

- **Scala.js 1.17.0**: Cross-compiles Scala to JavaScript
- **toon4s-core**: The core TOON format library (shared with JVM)
- **scalajs-dom**: DOM manipulation library for Scala.js
- **Pure Functional Design**: All conversion logic uses pure functions

### Key Components

**PlaygroundApp.scala**:
- Main entry point for the Scala.js application
- Handles DOM event listeners
- Manages conversion between JSON and TOON formats
- Calculates and displays statistics

**index.html**:
- Provides the structure for the UI
- Includes input/output text areas
- Example buttons and configuration controls
- Loads the compiled Scala.js code

**styles.css**:
- Modern, responsive design
- Gradient backgrounds and smooth animations
- Mobile-friendly layout

## Usage

### Converting JSON to TOON

1. Paste or type JSON in the "JSON Input" textarea
2. Select your preferred delimiter (comma, tab, or pipe)
3. Choose indentation (2, 4, or 8 spaces)
4. Click "Convert to TOON →"
5. View the TOON output and token savings statistics

### Converting TOON to JSON

1. Paste TOON format data in the "TOON Input" textarea
2. Click "Convert to JSON →"
3. View the JSON output

### Try Examples

Click any of the example buttons to load sample data:

- **Users Array**: Simple array of user objects with tags
- **Orders with Items**: Nested structure with order items
- **Product Details**: Complex object with specifications

## Deployment

### Local File System

The playground can be run directly from the file system. After building:

1. Run `sbt playground/fastLinkJS`
2. Open `playground/src/main/resources/index.html` in a browser

### Web Server

For hosting on a web server:

1. Build for production: `sbt playground/fullLinkJS`
2. Copy these files to your web root:
   - `playground/src/main/resources/index.html`
   - `playground/src/main/resources/styles.css`
   - `playground/target/scala-js-opt/toon4s-playground-opt.js`
3. Update the script path in `index.html` to point to the correct location

### GitHub Pages

To deploy on GitHub Pages:

1. Build the playground: `sbt playground/fullLinkJS`
2. Create a `docs/` or `gh-pages` branch
3. Copy the HTML, CSS, and compiled JS files
4. Enable GitHub Pages in repository settings

## Browser Compatibility

The playground works in all modern browsers:

- ✅ Chrome/Edge (Chromium)
- ✅ Firefox
- ✅ Safari
- ✅ Opera

## Performance

- **Fast compilation**: FastLinkJS compiles in seconds
- **Small bundle size**: ~500KB for the optimized build (including toon4s-core)
- **No dependencies**: Pure Scala.js with minimal DOM library
- **Instant conversion**: Real-time JSON ↔ TOON conversion

## Limitations

- Token estimation is character-based (actual LLM token counts may vary)
- Requires JavaScript enabled in the browser
- Large JSON files (>10MB) may cause browser slowdowns

## Contributing

Contributions are welcome! Areas for improvement:

- [ ] Add syntax highlighting for JSON/TOON
- [ ] Integrate real tokenizer (e.g., tiktoken via WASM)
- [ ] Add download buttons for outputs
- [ ] Support file uploads
- [ ] Add sharing links with URL encoding
- [ ] Add more examples
- [ ] Mobile app version

## License

MIT License - same as the parent toon4s project.

## Links

- [toon4s Repository](https://github.com/vim89/toon4s)
- [TOON Format Specification](https://github.com/toon-format/spec)
- [Scala.js Documentation](https://www.scala-js.org/)

## Credits

Built with ❤️ using:
- [toon4s](https://github.com/vim89/toon4s) by Vitthal Mirji
- [Scala.js](https://www.scala-js.org/)
- [TOON Format](https://github.com/toon-format/spec)

---

**Get a quick impression of what TOON is!** 🎨
