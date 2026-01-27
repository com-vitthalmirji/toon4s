#!/bin/bash

# toon4s Playground Build Script
# 
# Usage:
#   ./build.sh              - Fast build (development)
#   ./build.sh --prod       - Production build (optimized)
#   ./build.sh --watch      - Watch mode (auto-rebuild)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

echo "🎨 toon4s Playground Builder"
echo "=============================="
echo ""

build_fast() {
    echo "📦 Building playground (fast mode)..."
    sbt playground/fastLinkJS
    echo ""
    echo "✅ Build complete!"
    echo ""
    echo "🌐 Open playground:"
    echo "   file://$SCRIPT_DIR/src/main/resources/index.html"
    echo ""
}

build_prod() {
    echo "📦 Building playground (production mode)..."
    sbt playground/fullLinkJS
    echo ""
    echo "✅ Production build complete!"
    echo ""
    echo "⚠️  Don't forget to update index.html to use:"
    echo "   toon4s-playground-opt.js"
    echo ""
    echo "🌐 Open playground:"
    echo "   file://$SCRIPT_DIR/src/main/resources/index.html"
    echo ""
}

watch_mode() {
    echo "👀 Starting watch mode..."
    echo "   Changes will auto-compile on save"
    echo "   Press Ctrl+C to stop"
    echo ""
    sbt "~playground/fastLinkJS"
}

case "${1:-}" in
    --prod)
        build_prod
        ;;
    --watch)
        watch_mode
        ;;
    *)
        build_fast
        ;;
esac
