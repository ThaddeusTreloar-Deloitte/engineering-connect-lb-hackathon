"""
Load Balancer module.
Handles request forwarding and load balancing algorithms.
"""
import requests
from flask import Request, Response
from typing import Optional, Dict
from config import Config
from target_group import TargetGroup
from target import Target
from error_handler import handle_error


# Hop-by-hop headers that should not be forwarded
HOP_BY_HOP_HEADERS = {'connection', 'keep-alive', 'transfer-encoding'}


class LoadBalancer:
    """Handles load balancing and request forwarding."""
    
    def __init__(self, config: Config):
        """
        Initialize the load balancer.
        
        Args:
            config: The configuration object
        """
        self.config = config
        self.round_robin_counters = {}  # Track round robin state per target group
        # Session pool per target host for connection reuse
        self._sessions: Dict[str, requests.Session] = {}
    
    def select_target(self, target_group: TargetGroup, request: Request) -> Optional[Target]:
        """
        Select a target from the target group using the configured algorithm.
        
        Args:
            target_group: The target group to select from
            request: The incoming request (for sticky sessions, etc.)
            
        Returns:
            Selected target or None if no targets available
        """
        targets = target_group.get_targets()
        
        if not targets:
            return None
        
        algorithm = self.config.get_load_balancing_algorithm()
        
        if algorithm == 'ROUND_ROBIN':
            return self._round_robin(target_group, targets)
        elif algorithm == 'WEIGHTED':
            # Not implemented yet
            return targets[0] if targets else None
        elif algorithm == 'STICKY':
            # Not implemented yet
            return targets[0] if targets else None
        elif algorithm == 'LRT':
            # Not implemented yet
            return targets[0] if targets else None
        else:
            # Default to round robin
            return self._round_robin(target_group, targets)
    
    def _round_robin(self, target_group: TargetGroup, targets: list) -> Target:
        """
        Select the next target using round robin algorithm.
        
        Args:
            target_group: The target group
            targets: List of available targets
            
        Returns:
            Selected target
        """
        group_name = target_group.name
        
        # Initialize counter if not exists
        if group_name not in self.round_robin_counters:
            self.round_robin_counters[group_name] = 0
        
        # Get current index
        index = self.round_robin_counters[group_name]
        
        # Select target
        target = targets[index % len(targets)]
        
        # Increment counter for next request
        self.round_robin_counters[group_name] = (index + 1) % len(targets)
        
        return target
    
    def _get_session(self, target: Target) -> requests.Session:
        """
        Get or create a session for the target host.
        Sessions are reused to enable connection pooling.
        
        Args:
            target: The target to get a session for
            
        Returns:
            A requests.Session configured for connection pooling
        """
        host_port = f"{target.ip}:{target.port}"
        if host_port not in self._sessions:
            session = requests.Session()
            # Disable proxy detection to avoid overhead
            session.trust_env = False
            # Configure connection pooling
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=10,  # Number of connection pools to cache
                pool_maxsize=20,      # Max connections per pool
                max_retries=0         # Disable retries for load balancer
            )
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            self._sessions[host_port] = session
        return self._sessions[host_port]
    
    def forward_request(self, target: Target, request: Request, path: str) -> Response:
        """
        Forward a request to the target.
        
        Args:
            target: The target to forward to
            request: The incoming Flask request
            path: The rewritten path
            
        Returns:
            Response from the target or error response
        """
        try:
            # Construct full URL
            url = target.get_url(path)
            
            # Prepare request headers (exclude hop-by-hop headers)
            # Use set for O(1) lookup instead of list iteration
            headers = {
                key: value for key, value in request.headers
                if key.lower() not in HOP_BY_HOP_HEADERS
            }
            
            # Prepare request data
            data = request.get_data()
            
            # Prepare query string
            query_string = request.query_string.decode('utf-8')
            if query_string:
                url += '?' + query_string
            
            # Make request with timeout using connection pooling
            timeout = self.config.get_connection_timeout()
            session = self._get_session(target)
            
            response = session.request(
                method=request.method,
                url=url,
                headers=headers,
                data=data,
                timeout=timeout,
                allow_redirects=False
            )
            
            # Create Flask response
            flask_response = Response(
                response.content,
                status=response.status_code,
                headers=dict(response.headers)
            )
            
            return flask_response
            
        except requests.exceptions.Timeout:
            return handle_error(504, "Request timeout")
        except requests.exceptions.ConnectionError:
            return handle_error(502, "Connection error")
        except Exception as e:
            return handle_error(502, f"Error forwarding request: {str(e)}")

