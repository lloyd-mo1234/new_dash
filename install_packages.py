#!/usr/bin/env python3
"""
Swap Analytics Dashboard - Package Installation Script

This script installs all required Python packages for the dashboard
using CBA's internal artifactory repository.

Usage: python install_packages.py
"""

import subprocess
import sys
import importlib
import os
from typing import List, Tuple

# CBA-specific pip configuration
PIP_INDEX_URL = "https://artifactory.internal.cba/api/pypi/org.python.pypi/simple"
TRUSTED_HOST = "artifactory.internal.cba"

# Required packages with their import names
REQUIRED_PACKAGES = [
    ("Flask", "flask"),
    ("pandas", "pandas"),
    ("numpy", "numpy"),
    ("plotly", "plotly"),
    ("scikit-learn", "sklearn"),
    ("scipy", "scipy"),
    ("python-dateutil", "dateutil"),
    ("futures", "concurrent.futures")  # For older Python versions
]

# Built-in modules to verify
BUILTIN_MODULES = [
    "threading",
    "json",
    "re",
    "os",
    "glob",
    "datetime",
    "time",
    "hashlib",
    "functools"
]

def print_header():
    """Print the script header"""
    print("=" * 50)
    print("  Swap Rate Analytics Dashboard")
    print("  Package Installation Script")
    print("=" * 50)
    print()

def print_section(title: str):
    """Print a section header"""
    print(f"\n{title}")
    print("-" * len(title))

def install_package(package_name: str, display_name: str = None) -> bool:
    """
    Install a package using CBA's pip configuration
    
    Args:
        package_name: Name of package to install
        display_name: Display name for progress (defaults to package_name)
    
    Returns:
        bool: True if installation successful, False otherwise
    """
    if display_name is None:
        display_name = package_name
    
    print(f"Installing {display_name}...")
    
    cmd = [
        sys.executable, "-m", "pip", "install", package_name,
        "--user",
        f"--index-url={PIP_INDEX_URL}",
        f"--trusted-host={TRUSTED_HOST}"
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout per package
        )
        
        if result.returncode == 0:
            print(f"✓ {display_name} installed successfully")
            return True
        else:
            print(f"✗ Failed to install {display_name}")
            print(f"  Error: {result.stderr.strip()}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"✗ Installation of {display_name} timed out")
        return False
    except Exception as e:
        print(f"✗ Error installing {display_name}: {str(e)}")
        return False

def verify_package(import_name: str, display_name: str = None) -> bool:
    """
    Verify that a package can be imported
    
    Args:
        import_name: Name to use for import
        display_name: Display name (defaults to import_name)
    
    Returns:
        bool: True if package can be imported, False otherwise
    """
    if display_name is None:
        display_name = import_name
    
    try:
        importlib.import_module(import_name)
        return True
    except ImportError:
        return False

def install_all_packages() -> Tuple[int, int]:
    """
    Install all required packages
    
    Returns:
        Tuple[int, int]: (successful_installs, total_packages)
    """
    print_section("Installing Required Packages")
    print("Using CBA internal artifactory repository")
    print()
    
    successful = 0
    total = len(REQUIRED_PACKAGES)
    
    for i, (package_name, import_name) in enumerate(REQUIRED_PACKAGES, 1):
        print(f"[{i}/{total}] ", end="")
        
        # Check if already installed
        if verify_package(import_name):
            print(f"{package_name} is already installed ✓")
            successful += 1
            continue
        
        # Install the package
        if install_package(package_name):
            successful += 1
        
        print()  # Add spacing between packages
    
    return successful, total

def verify_all_packages() -> Tuple[int, int]:
    """
    Verify all packages are properly installed
    
    Returns:
        Tuple[int, int]: (successful_verifications, total_packages)
    """
    print_section("Package Verification Results")
    
    successful = 0
    total = len(REQUIRED_PACKAGES) + len(BUILTIN_MODULES)
    
    # Verify installed packages
    print("External Packages:")
    for package_name, import_name in REQUIRED_PACKAGES:
        if verify_package(import_name):
            print(f"✓ {package_name:<20} - OK")
            successful += 1
        else:
            print(f"✗ {package_name:<20} - MISSING")
    
    print("\nBuilt-in Modules:")
    # Verify built-in modules
    for module_name in BUILTIN_MODULES:
        if verify_package(module_name):
            print(f"✓ {module_name:<20} - OK")
            successful += 1
        else:
            print(f"✗ {module_name:<20} - MISSING")
    
    return successful, total

def print_cba_requirements():
    """Print information about CBA-specific requirements"""
    print_section("CBA-Specific Requirements")
    print("The following package is required but must be installed")
    print("separately through CBA internal systems:")
    print()
    print("  • cba.analytics.xcurves")
    print()
    print("Please ensure this package is available in your Python")
    print("environment before running the dashboard.")

def print_next_steps():
    """Print next steps for running the application"""
    print_section("Next Steps")
    print("1. Ensure cba.analytics.xcurves is installed through CBA systems")
    print()
    print("2. Navigate to the storage directory")
    print("3. Run the application:")
    print("   python app.py")
    print()
    print("4. Open your browser to:")
    print("   http://localhost:5000")
    print()

def check_python_version():
    """Check if Python version is compatible"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 6):
        print("⚠️  Warning: Python 3.6+ is recommended for best compatibility")
        print(f"   Current version: {version.major}.{version.minor}.{version.micro}")
        print()

def main():
    """Main installation function"""
    print_header()
    
    # Check Python version
    check_python_version()
    
    # Install packages
    successful_installs, total_packages = install_all_packages()
    
    print_section("Installation Summary")
    print(f"Successfully installed: {successful_installs}/{total_packages} packages")
    
    if successful_installs < total_packages:
        print("⚠️  Some packages failed to install. Please check the errors above.")
        print("   You may need to install them manually or contact IT support.")
    
    print()
    
    # Verify installations
    successful_verifications, total_verifications = verify_all_packages()
    
    print(f"\nVerification: {successful_verifications}/{total_verifications} modules available")
    
    # Print additional requirements and next steps
    print_cba_requirements()
    print_next_steps()
    
    print("=" * 50)
    
    if successful_installs == total_packages and successful_verifications >= len(REQUIRED_PACKAGES):
        print("🎉 Installation completed successfully!")
        print("   Your dashboard is ready to run.")
    else:
        print("⚠️  Installation completed with some issues.")
        print("   Please review the errors above before proceeding.")
    
    print("=" * 50)
    
    # Return appropriate exit code
    if successful_installs < total_packages:
        return 1
    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nInstallation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nUnexpected error: {str(e)}")
        print("Please contact support if this issue persists.")
        sys.exit(1)
