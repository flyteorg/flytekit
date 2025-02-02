import os
import pytest
import pickle
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
        profiling_data = {
            "segments": profiling_data.get("segments", []),
            "traces": profiling_data.get("traces", []),
            "allocator_settings": profiling_data.get("allocator_settings", {})
        }
    elif plot_type == "profile_plot":
        profiling_data = {
            "steps": [],
            "events": [],
            "key_averages": []
        }
    
    renderer = PyTorchProfilingRenderer(profiling_data)
    html_output = renderer.to_html(plot_type)
    
    # Basic HTML structure checks
    assert isinstance(html_output, str)
    assert "<html>" in html_output
    assert "<body>" in html_output
    
    # Check for expected content based on plot type
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

def test_render_pytorch_profiling_invalid_file():
    """Test render_pytorch_profiling with invalid pickle file"""
    # Create a temporary invalid pickle file
    invalid_file_path = os.path.join(CURRENT_DIR, "invalid.pkl")
    with open(invalid_file_path, "w") as f:
        f.write("not a pickle file")
    
    try:
        invalid_file = FlyteFile(invalid_file_path)
        with pytest.raises(ValueError, match="Failed to load profiling data"):
            render_pytorch_profiling(invalid_file)
    finally:
        # Clean up the temporary file
        if os.path.exists(invalid_file_path):
            os.remove(invalid_file_path)

def test_compare_plot_type():
    """Test the compare plot type which requires two snapshots"""
    with open(PROFILE_PATH, "rb") as f:
        profiling_data = pickle.load(f)
    
    # Create proper before/after snapshots with correct structure
    before = {
        "segments": [{
            "address": 0,
            "total_size": 0,
            "blocks": [],
            "stream": 0
        }],
        "traces": [],
        "allocator_settings": {
            "expandable_segments": False,
            "garbage_collection_threshold": 0
        }
    }
    
    # Ensure after data has the correct structure
    if isinstance(profiling_data, dict):
        after = profiling_data
    else:
        after = {
            "segments": profiling_data.get("segments", []) if hasattr(profiling_data, "get") else [],
            "traces": profiling_data.get("traces", []) if hasattr(profiling_data, "get") else [],
            "allocator_settings": profiling_data.get("allocator_settings", {}) if hasattr(profiling_data, "get") else {}
        }
    
    renderer = PyTorchProfilingRenderer((before, after))
    html_output = renderer.to_html("compare")
    
    assert isinstance(html_output, str)
    assert "<html>" in html_output
    assert "MemoryViz.js" in html_output