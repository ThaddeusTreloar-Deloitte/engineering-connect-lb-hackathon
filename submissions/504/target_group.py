"""
Target Group module.
Represents a set of targets that can be load balanced.
"""
import socket
from typing import List, Optional, Dict
from target import Target


class TargetGroup:
    """Represents a group of targets for load balancing."""
    
    def __init__(self, name: str, targets_str: str, weights: Optional[Dict[str, int]] = None):
        """
        Initialize a target group.
        
        Args:
            name: The name of the target group
            targets_str: Comma-delimited list of <hostname>:<port>/<base-uri> entries
            weights: Dictionary mapping hostname to weight (optional)
        """
        self.name = name
        # Store None explicitly to distinguish between "not provided" and "empty dict"
        self.weights = weights if weights is not None else {}
        self._weights_provided = weights is not None
        self.targets = self._parse_targets(targets_str)
    
    def _parse_targets(self, targets_str: str) -> List[Target]:
        """
        Parse targets from a comma-delimited string.
        Format: <hostname>:<port>/<base-uri>
        
        Args:
            targets_str: Comma-delimited list of target specifications
            
        Returns:
            List of Target objects
        """
        targets = []
        
        if not targets_str:
            return targets
        
        # Split by comma
        target_specs = [spec.strip() for spec in targets_str.split(',')]
        
        for spec in target_specs:
            if not spec:
                continue
            
            # Parse hostname:port/base-uri
            # First, check if there's a base URI
            if '/' in spec:
                # Split on the first / to separate address from base URI
                parts = spec.split('/', 1)
                address_part = parts[0]
                base_uri = '/' + parts[1] if parts[1] else '/'
            else:
                address_part = spec
                base_uri = '/'
            
            # Parse hostname:port
            if ':' in address_part:
                hostname, port_str = address_part.rsplit(':', 1)
                try:
                    port = int(port_str)
                except ValueError:
                    continue
            else:
                hostname = address_part
                port = 80  # Default HTTP port
            
            # Resolve DNS to get all IP addresses
            ip_addresses = self._resolve_dns(hostname)
            
            # Create a target for each IP address, preserving hostname for weight lookup
            for ip in ip_addresses:
                target = Target(ip, port, base_uri, hostname=hostname)
                targets.append(target)
        
        return targets
    
    def _resolve_dns(self, hostname: str) -> List[str]:
        """
        Resolve a hostname to one or more IP addresses.
        
        Args:
            hostname: The hostname to resolve
            
        Returns:
            List of IP addresses
        """
        ip_addresses = []
        
        try:
            # Try to resolve as IP address first
            socket.inet_aton(hostname)
            ip_addresses.append(hostname)
        except socket.error:
            # Not an IP address, try DNS resolution
            try:
                # Get all address info
                addr_infos = socket.getaddrinfo(hostname, None, socket.AF_INET)
                # Extract unique IP addresses
                seen_ips = set()
                for addr_info in addr_infos:
                    ip = addr_info[4][0]
                    if ip not in seen_ips:
                        seen_ips.add(ip)
                        ip_addresses.append(ip)
            except socket.gaierror:
                # DNS resolution failed
                pass
        
        return ip_addresses if ip_addresses else []
    
    def get_targets(self) -> List[Target]:
        """Get all targets in this group."""
        return self.targets
    
    def get_weight(self, hostname: str) -> int:
        """
        Get the weight for a hostname.
        
        Args:
            hostname: The hostname to get weight for
            
        Returns:
            The weight for the hostname, or 1 if not specified
        """
        return self.weights.get(hostname, 1)
    
    def get_weighted_target_list(self) -> List[Target]:
        """
        Get a list of targets expanded by their weights for weighted round-robin.
        Each target appears in the list a number of times equal to its weight.
        
        Returns:
            List of targets expanded by weight, or empty list if no weights configured
        """
        # If weights were not provided, return empty list
        if not self._weights_provided:
            return []
        
        weighted_list = []
        # Group targets by hostname to apply weights
        hostname_targets: Dict[str, List[Target]] = {}
        for target in self.targets:
            hostname = target.hostname or target.ip
            if hostname not in hostname_targets:
                hostname_targets[hostname] = []
            hostname_targets[hostname].append(target)
        
        # Build weighted list: each hostname's targets appear weight times
        for hostname, targets in hostname_targets.items():
            weight = self.get_weight(hostname)
            # Add all IPs for this hostname, repeated by weight
            for _ in range(weight):
                weighted_list.extend(targets)
        
        return weighted_list

