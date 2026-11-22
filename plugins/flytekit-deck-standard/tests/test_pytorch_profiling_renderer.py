import os
import pytest
import pickle
import tempfile
from flytekit.types.file import FlyteFile
from flytekitplugins.deck.renderer import PyTorchProfilingRenderer, render_pytorch_profiling

# Get the current directory where the test file is located
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROFILE_PATH = os.path.join(CURRENT_DIR, "profile1.pkl")

def test_pytorch_profiling_renderer_initialization():
    """Test PyTorchProfilingRenderer initialization"""
    with open(PROFILE_PATH, "rb") as f:
        profiling_data = pickle.load(f)
    
    renderer = PyTorchProfilingRenderer(profiling_data)
    assert renderer.profiling_data == profiling_data

def test_pytorch_profiling_renderer_invalid_data():
    """Test PyTorchProfilingRenderer with invalid data"""
    with pytest.raises(ValueError):
        PyTorchProfilingRenderer(None)

def _create_profiling_structure(data):
    """Creates a standardized profiling data structure.
    
    Args:
        data: Raw profiling data
        
    Returns:
        dict: Structured profiling data with required keys
    """
    return {
        "segments": data.get("segments", []) if hasattr(data, "get") else [],
        "traces": data.get("traces", []) if hasattr(data, "get") else [],
        "allocator_settings": data.get("allocator_settings", {}) if hasattr(data, "get") else {}
    }

@pytest.mark.parametrize("plot_type,expected_content", [
    ("trace_plot", "<!DOCTYPE html>"),
    ("segment_plot", "<!DOCTYPE html>"),
    ("memory", "MemoryViz.js"),
    ("segments", "MemoryViz.js"),
    ("profile_plot", "<div>")
])
def test_pytorch_profiling_renderer_plot_types(plot_type, expected_content):
    """Test different plot types for PyTorchProfilingRenderer"""
    with open(PROFILE_PATH, "rb") as f:
        profiling_data = pickle.load(f)
    
    # Convert profiling data to proper format if needed
    if plot_type in ["memory", "segments"]:
        profiling_data = _create_profiling_structure(profiling_data)
    elif plot_type == "profile_plot":
        profiling_data = {
            "steps": [],
            "events": [],
            "key_averages": []
        }
    
    renderer = PyTorchProfilingRenderer(profiling_data)
    html_output = renderer.to_html(plot_type)
    
    assert isinstance(html_output, str)
    assert "<html>" in html_output
    assert expected_content in html_output

def test_pytorch_profiling_renderer_invalid_plot_type():
    """Test PyTorchProfilingRenderer with invalid plot type"""
    with open(PROFILE_PATH, "rb") as f:
        profiling_data = pickle.load(f)
    
    renderer = PyTorchProfilingRenderer(profiling_data)
    with pytest.raises(ValueError, match="Unknown plot type"):
        renderer.to_html("invalid_plot_type")

def test_render_pytorch_profiling_function():
    """Test the render_pytorch_profiling helper function"""
    profiling_file = FlyteFile(PROFILE_PATH)
    
    # Test with default plot type
    html_output = render_pytorch_profiling(profiling_file)
    assert isinstance(html_output, str)
    assert "<html>" in html_output
    
    # Test with specific plot type
    html_output = render_pytorch_profiling(profiling_file, plot_type="memory")
    assert isinstance(html_output, str)
    assert "<html>" in html_output

def test_render_pytorch_profiling_file_not_found():
    """Test render_pytorch_profiling with non-existent file"""
    non_existent_file = FlyteFile("non_existent.pkl")
    with pytest.raises(ValueError, match="Failed to load profiling data"):
        render_pytorch_profiling(non_existent_file)

def test_invalid_file_handling():
    """Test handling of invalid profiling data files."""
    
    # Use tempfile context manager for safe cleanup
    with tempfile.NamedTemporaryFile(mode='w', suffix='.pkl', delete=False) as temp_file:
        # Write invalid data
        temp_file.write("not a pickle file")
        temp_file.flush()
        
        # Create FlyteFile from temp file
        invalid_file = FlyteFile(temp_file.name)
        
        # Test that it raises the expected error
        with pytest.raises(ValueError, match="Failed to load profiling data"):
            render_pytorch_profiling(invalid_file)
            
    # No need for manual cleanup - tempfile handles it automatically

def test_compare_plot_type():
    """Test the compare plot type which requires two snapshots"""
    with open(PROFILE_PATH, "rb") as f:
        profiling_data = pickle.load(f)
    
    # Create proper before/after snapshots
    before = _create_profiling_structure({})  # Empty snapshot
    after = _create_profiling_structure(profiling_data)  # Your actual data
    
    renderer = PyTorchProfilingRenderer((before, after))
    html_output = renderer.to_html("compare")
    
    assert isinstance(html_output, str)
    assert "<html>" in html_output
    assert "MemoryViz.js" in html_output