#!/usr/bin/env python3
"""
Network utilities for IP address generation and management
"""

import ipaddress
import json
from typing import List, Dict, Any, Iterator
from dataclasses import dataclass
import random
import time
import hashlib


# Sample data for realistic IP geolocation
SAMPLE_LOCATIONS = [
    {"country_code": "US", "country_name": "United States", "state": "California", "city": "San Francisco", "zip_code": "94102", "lat": 37.7749, "lng": -122.4194, "tz": "America/Los_Angeles", "utc_offset": -28800},
    {"country_code": "US", "country_name": "United States", "state": "New York", "city": "New York", "zip_code": "10001", "lat": 40.7128, "lng": -74.0060, "tz": "America/New_York", "utc_offset": -18000},
    {"country_code": "US", "country_name": "United States", "state": "Texas", "city": "Austin", "zip_code": "73301", "lat": 30.2672, "lng": -97.7431, "tz": "America/Chicago", "utc_offset": -21600},
    {"country_code": "GB", "country_name": "United Kingdom", "state": "England", "city": "London", "zip_code": "SW1A 1AA", "lat": 51.5074, "lng": -0.1278, "tz": "Europe/London", "utc_offset": 0},
    {"country_code": "DE", "country_name": "Germany", "state": "Bavaria", "city": "Munich", "zip_code": "80331", "lat": 48.1351, "lng": 11.5820, "tz": "Europe/Berlin", "utc_offset": 3600},
    {"country_code": "JP", "country_name": "Japan", "state": "Tokyo", "city": "Tokyo", "zip_code": "100-0001", "lat": 35.6762, "lng": 139.6503, "tz": "Asia/Tokyo", "utc_offset": 32400},
    {"country_code": "AU", "country_name": "Australia", "state": "New South Wales", "city": "Sydney", "zip_code": "2000", "lat": -33.8688, "lng": 151.2093, "tz": "Australia/Sydney", "utc_offset": 36000},
    {"country_code": "CA", "country_name": "Canada", "state": "Ontario", "city": "Toronto", "zip_code": "M5H 2N2", "lat": 43.6532, "lng": -79.3832, "tz": "America/Toronto", "utc_offset": -18000},
    {"country_code": "FR", "country_name": "France", "state": "Île-de-France", "city": "Paris", "zip_code": "75001", "lat": 48.8566, "lng": 2.3522, "tz": "Europe/Paris", "utc_offset": 3600},
    {"country_code": "BR", "country_name": "Brazil", "state": "São Paulo", "city": "São Paulo", "zip_code": "01310-100", "lat": -23.5505, "lng": -46.6333, "tz": "America/Sao_Paulo", "utc_offset": -10800},
    {"country_code": "IN", "country_name": "India", "state": "Maharashtra", "city": "Mumbai", "zip_code": "400001", "lat": 19.0760, "lng": 72.8777, "tz": "Asia/Kolkata", "utc_offset": 19800},
    {"country_code": "SG", "country_name": "Singapore", "state": "Singapore", "city": "Singapore", "zip_code": "018989", "lat": 1.3521, "lng": 103.8198, "tz": "Asia/Singapore", "utc_offset": 28800},
]

NETWORK_TYPES = ["private", "business", "residential", "mobile", "hosting", "education", "government"]
ISPS = ["Comcast", "Verizon", "AT&T", "Charter", "CenturyLink", "Cox", "Optimum", "Spectrum", "Xfinity", "T-Mobile", "Amazon AWS", "Google Cloud", "Microsoft Azure", "DigitalOcean", "Cloudflare"]
ORGANIZATIONS = ["Enterprise Corp", "Tech Solutions Inc", "Global Networks Ltd", "Data Systems LLC", "Cloud Services Co", "Internet Provider Inc", "Telecom Solutions", "Business Networks", "Hosting Services", "ISP Corporation"]


# Extended data for larger keys
CONNECTION_TYPES = ["cable", "dsl", "fiber", "satellite", "cellular", "t1", "t3", "ethernet", "wireless"]
USAGE_TYPES = ["commercial", "residential", "educational", "government", "military", "healthcare", "financial"]
BANDWIDTH_TIERS = ["low", "medium", "high", "enterprise", "premium", "unlimited"]
CARRIERS = ["Verizon Wireless", "AT&T Mobility", "T-Mobile USA", "Sprint", "US Cellular", "Cricket", "Metro PCS"]
LINE_SPEEDS = ["56k", "128k", "256k", "512k", "1Mbps", "5Mbps", "10Mbps", "25Mbps", "50Mbps", "100Mbps", "1Gbps"]
PRIVACY_LEVELS = ["public", "restricted", "private", "confidential", "classified"]

REGIONS = ["North America", "South America", "Europe", "Asia Pacific", "Middle East", "Africa", "Oceania"]
TAGS = ["datacenter", "residential", "mobile", "vpn", "proxy", "tor", "malware", "botnet", "scanner", "legitimate"]

# Sample ASN data
ASN_DATA = [
    {"asn": 15169, "name": "Google LLC"},
    {"asn": 8075, "name": "Microsoft Corporation"},
    {"asn": 16509, "name": "Amazon.com Inc"},
    {"asn": 13335, "name": "Cloudflare Inc"},
    {"asn": 7922, "name": "Comcast Cable Communications"},
    {"asn": 701, "name": "Verizon Business"},
    {"asn": 7018, "name": "AT&T Services Inc"},
    {"asn": 20115, "name": "Charter Communications"},
    {"asn": 3356, "name": "Level 3 Parent LLC"},
    {"asn": 174, "name": "Cogent Communications"}
]

# Sample domains and hostnames
DOMAINS = ["example.com", "test.org", "sample.net", "demo.co", "corp.internal", "business.local"]
DNS_SERVERS = ["8.8.8.8", "8.8.4.4", "1.1.1.1", "1.0.0.1", "208.67.222.222", "208.67.220.220"]

# Sample notes templates
NOTES_TEMPLATES = [
    "High-traffic IP address with consistent usage patterns. Monitored for security compliance.",
    "Corporate network endpoint with standard business applications. Regular security scans performed.",
    "Residential broadband connection with typical consumer usage. No security concerns identified.",
    "Mobile device connection with variable location data. Standard carrier security policies applied.",
    "Data center hosting environment with multiple virtual instances. Enhanced monitoring enabled.",
    "Educational institution network with student and faculty access. Content filtering active.",
    "Government network segment with restricted access policies. High security classification required."
]


def generate_deterministic_location(ip_str: str) -> Dict[str, Any]:
    """Generate deterministic but realistic location data with enhanced geographic details"""
    # Use IP as seed for consistent results
    hash_obj = hashlib.md5(ip_str.encode())
    seed = int(hash_obj.hexdigest()[:8], 16)
    random.seed(seed)

    location = random.choice(SAMPLE_LOCATIONS)

    # Add some variation to coordinates (within ~10km)
    lat_variation = random.uniform(-0.1, 0.1)
    lng_variation = random.uniform(-0.1, 0.1)

    # Generate additional geographic details
    region = random.choice(REGIONS)
    area_code = str(random.randint(200, 999))
    metro_code = random.randint(500, 900)

    # Enhanced postal code
    base_postal = location["zip_code"]
    if location["country_code"] == "US":
        postal_code = f"{base_postal}-{random.randint(1000, 9999)}"
    else:
        postal_code = base_postal

    return {
        "country_code": location["country_code"],
        "country_name": location["country_name"],
        "state": location["state"],
        "city": location["city"],
        "zip_code": location["zip_code"],
        "latitude": round(location["lat"] + lat_variation, 6),
        "longitude": round(location["lng"] + lng_variation, 6),
        "region": region,
        "postal_code": postal_code,
        "area_code": area_code,
        "metro_code": metro_code,
        "timezone_id": location["tz"],
        "utc_offset": location["utc_offset"],
        "dst_active": random.choice([True, False])
    }


def generate_network_info(ip_str: str, is_private: bool) -> Dict[str, Any]:
    """Generate deterministic network classification data with extensive metadata"""
    hash_obj = hashlib.md5(f"network_{ip_str}".encode())
    seed = int(hash_obj.hexdigest()[:8], 16)
    random.seed(seed)

    # Basic network info
    if is_private:
        network_type = "private"
        isp = "Internal Network"
        organization = "Private Organization"
        domain = "internal.local"
        hostname = f"host-{ip_str.replace('.', '-')}.internal.local"
    else:
        network_type = random.choice(NETWORK_TYPES[1:])  # Exclude 'private'
        isp = random.choice(ISPS)
        organization = random.choice(ORGANIZATIONS)
        domain = random.choice(DOMAINS)
        hostname = f"host-{ip_str.replace('.', '-')}.{domain}"

    # ASN data
    asn_info = random.choice(ASN_DATA)

    # Generate extensive metadata
    return {
        "network_type": network_type,
        "isp": isp,
        "organization": organization,
        "asn": asn_info["asn"],
        "asn_name": asn_info["name"],
        "connection_type": random.choice(CONNECTION_TYPES),
        "usage_type": random.choice(USAGE_TYPES),
        "domain": domain,
        "hostname": hostname,
        "carrier": random.choice(CARRIERS) if network_type == "mobile" else "",
        "line_speed": random.choice(LINE_SPEEDS),
        "static_ip": random.choice([True, False]),

        # Security data
        "vpn_detected": random.choice([True, False]) if not is_private else False,
        "proxy_detected": random.choice([True, False]) if not is_private else False,
        "reputation_score": round(random.uniform(0.0, 100.0), 2),
        "last_seen_malware": "never" if random.random() > 0.3 else "2024-09-15T10:30:00Z",
        # Traffic patterns
        "bandwidth_tier": random.choice(BANDWIDTH_TIERS),
        "estimated_users": random.randint(1, 500),

        # Compliance
        "gdpr_applicable": random.choice([True, False]),
        "data_retention_days": random.choice([30, 90, 180, 365, 1095]),
        "privacy_level": random.choice(PRIVACY_LEVELS),

        # Technical details
        "ip_version": 4,
        "subnet_mask": "255.255.255.0",
        "gateway": ".".join(ip_str.split(".")[:-1] + ["1"]),
        "dns_servers": random.sample(DNS_SERVERS, random.randint(2, 4)),

        # Additional context
        "notes": random.choice(NOTES_TEMPLATES),
        "tags": random.sample(TAGS, random.randint(2, 5)),
        "custom_fields": {
            "scan_frequency": random.choice(["daily", "weekly", "monthly"]),
            "monitoring_level": random.choice(["basic", "enhanced", "premium"]),
            "compliance_status": random.choice(["compliant", "pending", "non-compliant"]),
            "last_updated": "2024-09-22T17:30:00Z",
            "data_source": "LocationFlex-Enhanced-v1.0"
        }
    }


@dataclass
class IPInfo:
    """Enhanced information about an IP address with geolocation and network details"""
    ip: str
    network: str
    is_private: bool
    is_multicast: bool
    is_reserved: bool
    is_loopback: bool
    is_link_local: bool
    timestamp: float

    # Geographic information
    country_code: str
    country_name: str
    state: str
    city: str
    zip_code: str
    latitude: float
    longitude: float

    # Timezone information
    timezone_id: str
    utc_offset: int  # in seconds
    dst_active: bool

    # Network classification
    network_type: str  # 'private', 'business', 'residential', 'mobile', 'hosting'
    isp: str
    organization: str

    # Additional metadata
    vpn_detected: bool
    proxy_detected: bool

    # Extended metadata for larger keys
    asn: int  # Autonomous System Number
    asn_name: str  # AS organization name
    connection_type: str  # 'cable', 'dsl', 'fiber', 'satellite', 'cellular'
    usage_type: str  # 'commercial', 'residential', 'educational', 'government'
    domain: str  # Associated domain
    hostname: str  # Reverse DNS hostname

    # Security and reputation data
    reputation_score: float  # 0.0 to 100.0
    last_seen_malware: str  # ISO timestamp or 'never'

    # Traffic and usage patterns
    bandwidth_tier: str  # 'low', 'medium', 'high', 'enterprise'

    # Additional geographic details
    region: str  # Geographic region
    postal_code: str  # More detailed postal code
    area_code: str  # Phone area code
    metro_code: int  # Metropolitan area code

    # ISP and network details
    carrier: str  # Mobile carrier (if applicable)
    line_speed: str  # Connection speed estimate
    static_ip: bool  # Whether IP is static or dynamic

    # Technical metadata
    ip_version: int  # 4 or 6
    subnet_mask: str  # Network subnet mask
    gateway: str  # Default gateway
    dns_servers: List[str]  # Associated DNS servers

    # Additional context
    notes: str  # Free-form notes field
    tags: List[str]  # Classification tags
    custom_fields: Dict[str, str]  # Extensible custom data

    def to_dict(self) -> Dict[str, Any]:
        """Convert IPInfo to dictionary for JSON serialization"""
        return {
            "ip": self.ip,
            "network": self.network,
            "is_private": self.is_private,
            "is_multicast": self.is_multicast,
            "is_reserved": self.is_reserved,
            "is_loopback": self.is_loopback,
            "is_link_local": self.is_link_local,
            "timestamp": self.timestamp,

            # Geographic information
            "country_code": self.country_code,
            "country_name": self.country_name,
            "state": self.state,
            "city": self.city,
            "zip_code": self.zip_code,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "region": self.region,
            "postal_code": self.postal_code,
            "area_code": self.area_code,
            "metro_code": self.metro_code,

            # Timezone information
            "timezone_id": self.timezone_id,
            "utc_offset": self.utc_offset,
            "dst_active": self.dst_active,

            # Network classification
            "network_type": self.network_type,
            "isp": self.isp,
            "organization": self.organization,
            "asn": self.asn,
            "asn_name": self.asn_name,
            "connection_type": self.connection_type,
            "usage_type": self.usage_type,
            "domain": self.domain,
            "hostname": self.hostname,
            "carrier": self.carrier,
            "line_speed": self.line_speed,
            "static_ip": self.static_ip,

            # Security metadata
            "vpn_detected": self.vpn_detected,
            "proxy_detected": self.proxy_detected,
            "reputation_score": self.reputation_score,
            "last_seen_malware": self.last_seen_malware,

            # Traffic and usage patterns
            "bandwidth_tier": self.bandwidth_tier,

            # Technical metadata
            "ip_version": self.ip_version,
            "subnet_mask": self.subnet_mask,
            "gateway": self.gateway,
            "dns_servers": self.dns_servers,

            # Additional context
            "notes": self.notes,
            "tags": self.tags,
            "custom_fields": self.custom_fields
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IPInfo':
        """Create from dictionary"""
        return cls(**data)


class NetworkBlockGenerator:
    """Generate network blocks for distributed processing"""
    
    def __init__(self, exclude_private: bool = False, exclude_multicast: bool = True, 
                 exclude_reserved: bool = True):
        self.exclude_private = exclude_private
        self.exclude_multicast = exclude_multicast
        self.exclude_reserved = exclude_reserved
    
    def generate_public_blocks(self, block_size: int = 24) -> Iterator[str]:
        """
        Generate public IP network blocks
        
        Args:
            block_size: Network prefix length (e.g., 24 for /24 networks)
            
        Yields:
            Network blocks in CIDR notation
        """
        # Define major public IP ranges (simplified)
        public_ranges = [
            "1.0.0.0/8",      # APNIC
            "8.0.0.0/8",      # Level 3 (partial)
            "14.0.0.0/8",     # APNIC
            "27.0.0.0/8",     # APNIC
            "39.0.0.0/8",     # APNIC
            "58.0.0.0/8",     # APNIC
            "59.0.0.0/8",     # APNIC
            "60.0.0.0/8",     # APNIC
            "61.0.0.0/8",     # APNIC
            "101.0.0.0/8",    # APNIC
            "103.0.0.0/8",    # APNIC
            "106.0.0.0/8",    # APNIC
            "110.0.0.0/8",    # APNIC
            "111.0.0.0/8",    # APNIC
            "112.0.0.0/8",    # APNIC
            "113.0.0.0/8",    # APNIC
            "114.0.0.0/8",    # APNIC
            "115.0.0.0/8",    # APNIC
            "116.0.0.0/8",    # APNIC
            "117.0.0.0/8",    # APNIC
            "118.0.0.0/8",    # APNIC
            "119.0.0.0/8",    # APNIC
            "120.0.0.0/8",    # APNIC
            "121.0.0.0/8",    # APNIC
            "122.0.0.0/8",    # APNIC
            "123.0.0.0/8",    # APNIC
            "124.0.0.0/8",    # APNIC
            "125.0.0.0/8",    # APNIC
            "126.0.0.0/8",    # APNIC
        ]
        
        for range_cidr in public_ranges:
            network = ipaddress.IPv4Network(range_cidr, strict=False)
            
            # Generate subnets of the specified size
            try:
                for subnet in network.subnets(new_prefix=block_size):
                    if self._should_include_network(subnet):
                        yield str(subnet)
            except ValueError:
                # If block_size is smaller than the parent network, yield the parent
                if network.prefixlen <= block_size and self._should_include_network(network):
                    yield str(network)
    
    def generate_test_blocks(self, count: int = 10, block_size: int = 24) -> List[str]:
        """
        Generate a limited set of test network blocks
        
        Args:
            count: Number of blocks to generate
            block_size: Network prefix length
            
        Returns:
            List of network blocks in CIDR notation
        """
        test_ranges = [
            "8.8.8.0/24",      # Google DNS
            "1.1.1.0/24",      # Cloudflare DNS
            "208.67.222.0/24", # OpenDNS
            "4.4.4.0/24",      # Level3 DNS
            "9.9.9.0/24",      # Quad9 DNS
            "64.6.64.0/24",    # Verisign DNS
            "77.88.8.0/24",    # Yandex DNS
            "156.154.70.0/24", # Neustar DNS
            "198.51.100.0/24", # Test network (RFC 5737)
            "203.0.113.0/24",  # Test network (RFC 5737)
        ]
        
        return test_ranges[:count]
    
    def _should_include_network(self, network: ipaddress.IPv4Network) -> bool:
        """Check if a network should be included based on filters"""
        # Get a sample IP from the network to check properties
        sample_ip = network.network_address
        
        if self.exclude_private and sample_ip.is_private:
            return False
        
        if self.exclude_multicast and sample_ip.is_multicast:
            return False
        
        if self.exclude_reserved and sample_ip.is_reserved:
            return False
        
        # Additional exclusions
        if sample_ip.is_loopback or sample_ip.is_link_local:
            return False
        
        return True


class IPAddressGenerator:
    """Generate IP addresses from network blocks"""
    
    def __init__(self, network_block: str):
        self.network = ipaddress.IPv4Network(network_block, strict=False)
        self.network_str = network_block
    
    def generate_all_ips(self) -> Iterator[IPInfo]:
        """
        Generate all IP addresses in the network block with enhanced geolocation data

        Yields:
            IPInfo objects for each IP address with ~1KB of data
        """
        current_time = time.time()

        for ip in self.network.hosts():
            ip_str = str(ip)

            # Generate location data
            location_data = generate_deterministic_location(ip_str)

            # Generate network classification data
            network_data = generate_network_info(ip_str, ip.is_private)

            yield IPInfo(
                ip=ip_str,
                network=self.network_str,
                is_private=ip.is_private,
                is_multicast=ip.is_multicast,
                is_reserved=ip.is_reserved,
                is_loopback=ip.is_loopback,
                is_link_local=ip.is_link_local,
                timestamp=current_time,

                # Geographic information
                country_code=location_data["country_code"],
                country_name=location_data["country_name"],
                state=location_data["state"],
                city=location_data["city"],
                zip_code=location_data["zip_code"],
                latitude=location_data["latitude"],
                longitude=location_data["longitude"],
                region=location_data["region"],
                postal_code=location_data["postal_code"],
                area_code=location_data["area_code"],
                metro_code=location_data["metro_code"],

                # Timezone information
                timezone_id=location_data["timezone_id"],
                utc_offset=location_data["utc_offset"],
                dst_active=location_data["dst_active"],

                # Network classification
                network_type=network_data["network_type"],
                isp=network_data["isp"],
                organization=network_data["organization"],
                asn=network_data["asn"],
                asn_name=network_data["asn_name"],
                connection_type=network_data["connection_type"],
                usage_type=network_data["usage_type"],
                domain=network_data["domain"],
                hostname=network_data["hostname"],
                carrier=network_data["carrier"],
                line_speed=network_data["line_speed"],
                static_ip=network_data["static_ip"],

                # Security metadata
                vpn_detected=network_data["vpn_detected"],
                proxy_detected=network_data["proxy_detected"],
                reputation_score=network_data["reputation_score"],
                last_seen_malware=network_data["last_seen_malware"],

                # Traffic and usage patterns
                bandwidth_tier=network_data["bandwidth_tier"],

                # Technical metadata
                ip_version=network_data["ip_version"],
                subnet_mask=network_data["subnet_mask"],
                gateway=network_data["gateway"],
                dns_servers=network_data["dns_servers"],

                # Additional context
                notes=network_data["notes"],
                tags=network_data["tags"],
                custom_fields=network_data["custom_fields"]
            )
    
    def generate_sample_ips(self, count: int) -> List[IPInfo]:
        """
        Generate a random sample of IP addresses from the network
        
        Args:
            count: Number of IP addresses to generate
            
        Returns:
            List of IPInfo objects
        """
        all_ips = list(self.network.hosts())
        if len(all_ips) <= count:
            # Return all IPs if count is larger than available IPs
            return [self._create_ip_info(ip) for ip in all_ips]
        
        # Random sample
        sample_ips = random.sample(all_ips, count)
        return [self._create_ip_info(ip) for ip in sample_ips]
    
    def _create_ip_info(self, ip: ipaddress.IPv4Address) -> IPInfo:
        """Create enhanced IPInfo object for an IP address"""
        ip_str = str(ip)
        current_time = time.time()

        # Generate location data
        location_data = generate_deterministic_location(ip_str)

        # Generate network classification data
        network_data = generate_network_info(ip_str, ip.is_private)

        return IPInfo(
            ip=ip_str,
            network=self.network_str,
            is_private=ip.is_private,
            is_multicast=ip.is_multicast,
            is_reserved=ip.is_reserved,
            is_loopback=ip.is_loopback,
            is_link_local=ip.is_link_local,
            timestamp=current_time,

            # Geographic information
            country_code=location_data["country_code"],
            country_name=location_data["country_name"],
            state=location_data["state"],
            city=location_data["city"],
            zip_code=location_data["zip_code"],
            latitude=location_data["latitude"],
            longitude=location_data["longitude"],
            region=location_data["region"],
            postal_code=location_data["postal_code"],
            area_code=location_data["area_code"],
            metro_code=location_data["metro_code"],

            # Timezone information
            timezone_id=location_data["timezone_id"],
            utc_offset=location_data["utc_offset"],
            dst_active=location_data["dst_active"],

            # Network classification
            network_type=network_data["network_type"],
            isp=network_data["isp"],
            organization=network_data["organization"],
            asn=network_data["asn"],
            asn_name=network_data["asn_name"],
            connection_type=network_data["connection_type"],
            usage_type=network_data["usage_type"],
            domain=network_data["domain"],
            hostname=network_data["hostname"],
            carrier=network_data["carrier"],
            line_speed=network_data["line_speed"],
            static_ip=network_data["static_ip"],

            # Security metadata
            vpn_detected=network_data["vpn_detected"],
            proxy_detected=network_data["proxy_detected"],
            reputation_score=network_data["reputation_score"],
            last_seen_malware=network_data["last_seen_malware"],

            # Traffic and usage patterns
            bandwidth_tier=network_data["bandwidth_tier"],

            # Technical metadata
            ip_version=network_data["ip_version"],
            subnet_mask=network_data["subnet_mask"],
            gateway=network_data["gateway"],
            dns_servers=network_data["dns_servers"],

            # Additional context
            notes=network_data["notes"],
            tags=network_data["tags"],
            custom_fields=network_data["custom_fields"]
        )
    
    def get_network_info(self) -> Dict[str, Any]:
        """Get information about the network block"""
        return {
            "network": self.network_str,
            "network_address": str(self.network.network_address),
            "broadcast_address": str(self.network.broadcast_address),
            "netmask": str(self.network.netmask),
            "prefix_length": self.network.prefixlen,
            "num_addresses": self.network.num_addresses,
            "num_hosts": len(list(self.network.hosts())),
            "is_private": self.network.is_private,
        }


def create_redis_key(ip_info: IPInfo, version: str = "v22") -> str:
    """Create a Redis key for an IP address with version (legacy)"""
    return f"ip:{version}:{ip_info.ip}"


def create_redis_key_simple(key_id: int, version: str = "v22") -> str:
    """Create a Redis key using simple integer ID"""
    return f"ip:{version}:{key_id}"


def create_redis_value(ip_info: IPInfo) -> str:
    """Create a Redis value for an IP address (JSON)"""
    return json.dumps(ip_info.to_dict())


def generate_random_key_id(max_daily_keys: int = 200000) -> int:
    """Generate a random key ID between 0 and max_daily_keys"""
    return random.randint(0, max_daily_keys - 1)


def should_skip_write(skip_probability: float = 0.05) -> bool:
    """Determine if we should skip writing this key (for cache miss simulation)"""
    return random.random() < skip_probability


if __name__ == "__main__":
    # Example usage
    print("🌐 Network Utilities Example")
    print("-" * 40)
    
    # Generate test blocks
    generator = NetworkBlockGenerator()
    test_blocks = generator.generate_test_blocks(5)
    
    print(f"Generated {len(test_blocks)} test network blocks:")
    for block in test_blocks:
        print(f"  - {block}")
    
    # Generate IPs from first block
    if test_blocks:
        print(f"\nGenerating IPs from {test_blocks[0]}:")
        ip_gen = IPAddressGenerator(test_blocks[0])
        
        # Show network info
        net_info = ip_gen.get_network_info()
        print(f"Network info: {net_info['num_hosts']} hosts")
        
        # Generate sample IPs
        sample_ips = ip_gen.generate_sample_ips(5)
        for ip_info in sample_ips:
            key = create_redis_key(ip_info, "v22")  # Example with version
            value = create_redis_value(ip_info)
            print(f"  {key} -> {value}")
