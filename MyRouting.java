/*******************

There are some print statements that can help understand what is going on
on line 147 there are prints to show what the links found look like from the Link Discovery Service
on line 288 there are prints to show what the shortest path from the source switch
to all other switches are using Dijkstra's algorithm 
on line 321 there are prints to show what the link path used for creating new Routes to install

*******************/

package net.floodlightcontroller.myrouting;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFPacketIn;
import org.openflow.protocol.OFType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import java.util.ArrayList;
import java.util.Set;

import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.linkdiscovery.LinkInfo;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.routing.RouteId;
import net.floodlightcontroller.staticflowentry.IStaticFlowEntryPusherService;
import net.floodlightcontroller.topology.NodePortTuple;

import org.openflow.util.HexString;
import org.openflow.util.U8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyRouting implements IOFMessageListener, IFloodlightModule {

	protected IFloodlightProviderService floodlightProvider;
	protected Set<Long> macAddresses;
	protected static Logger logger;
	protected IDeviceService deviceProvider;
	protected ILinkDiscoveryService linkProvider;
	
	protected Map<Long,Set<Long>> topology = new HashMap<>();
	protected Map<String, Short> portLinks = new HashMap<>();

	protected Map<Long, IOFSwitch> switches;
	protected Map<Link, LinkInfo> links;
	protected Collection<? extends IDevice> devices;

	protected static int uniqueFlow;
	protected ILinkDiscoveryService lds;
	protected IStaticFlowEntryPusherService flowPusher;
	protected boolean printedTopo = false;

	@Override
	public String getName() {
		return MyRouting.class.getSimpleName();
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		return (type.equals(OFType.PACKET_IN)
				&& (name.equals("devicemanager") || name.equals("topology")) || name
					.equals("forwarding"));
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		return false;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(IDeviceService.class);
		l.add(ILinkDiscoveryService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProvider = context
				.getServiceImpl(IFloodlightProviderService.class);
		deviceProvider = context.getServiceImpl(IDeviceService.class);
		linkProvider = context.getServiceImpl(ILinkDiscoveryService.class);
		flowPusher = context
				.getServiceImpl(IStaticFlowEntryPusherService.class);
		lds = context.getServiceImpl(ILinkDiscoveryService.class);

	}

	@Override
	public void startUp(FloodlightModuleContext context) {
		floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
	}

	@Override
	public net.floodlightcontroller.core.IListener.Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		
		// Print the topology if not yet.
		if (!printedTopo) {
			System.out.println("*** Print topology");
			
			// For each switch, print its neighbor switches.
			
			for(NodePortTuple node : linkProvider.getPortLinks().keySet())
			{
				/* Testing info 
				System.out.println(node.toString());
				System.out.println(linkProvider.getPortLinks().get(node));
				*/
				
				Iterator<Link> it = linkProvider.getPortLinks().get(node).iterator();
				while(it.hasNext())
				{
					Link link = it.next();
					
					if(!topology.containsKey(link.getSrc()))
					{
						topology.put(link.getSrc(), new HashSet<Long>());
					}
					
					topology.get(link.getSrc()).add(link.getDst());
					
					//now if we want to get from one switch to another then we can find out what port to use
					//this help us when we want to build the route
					String portLink = link.getSrc() + "-" + link.getDst();
					portLinks.put(portLink, link.getSrcPort());
					
					//what the key for the map of out port looks like
					//System.out.println(portLink + " -> " + portLinks.get(portLink));
				}
			}
			
			for(Long swi : topology.keySet())
			{
				String neighbors = topology.get(swi).toString();
				System.out.println("switch " + swi + " neighbors: " + neighbors.substring(1, neighbors.length() - 1));
			}
			
			printedTopo = true;
		}


		// eth is the packet sent by a switch and received by floodlight.
		Ethernet eth = IFloodlightProviderService.bcStore.get(cntx,
				IFloodlightProviderService.CONTEXT_PI_PAYLOAD);

		// We process only IP packets of type 0x0800.
		if (eth.getEtherType() != 0x0800) {
			return Command.CONTINUE;
		}
		else{
			System.out.println("*** New flow packet");

			// Parse the incoming packet.
			OFPacketIn pi = (OFPacketIn)msg;
			OFMatch match = new OFMatch();
		    match.loadFromPacket(pi.getPacketData(), pi.getInPort());	
			
			// Obtain source and destination IPs.
			// ...
		    
			System.out.println("srcIP: " + match.getNetworkSourceCIDR().substring(0,8));
	        System.out.println("dstIP: " + match.getNetworkDestinationCIDR().substring(0,8));


			// Calculate the path using Dijkstra's algorithm.
	        //We can use the MAC addresses here since if we know what MAC address was used to send the packet
	        //then it must match up with the switch that it is using
	        //if h1 sends a packet through 00:00:00:00:01 then it hits the switch that matches
	        //here it is fairly simple since each host is connected to a switch that matches appropriately
	        //otherwise we would have to map sending IP's and MAC addresses or we would have to create
	        //sub nets so that wee know that a switch will only receive IP's from 10.0.0.0/8 or 10.0.1.0/8
	        //which is what is happening, switch 1 receives all packets coming from 10.0.1.0/8
	        //so in the future it might be best to use this to figure out which switch is being used
	        //no rules state this, it is just a noticeable pattern and since there are only 4 hosts and 4 switches
	        //we will assume for now that all switches are tied to one host and we can get this from the MAC address
	        
	        String pathRoute = calculateRoute(eth.getSourceMAC().toLong(), eth.getDestinationMAC().toLong());
	        
			Route route = getRoute(eth.getSourceMAC().toLong(), eth.getDestinationMAC().toLong(), pathRoute);
			StringBuilder sb = new StringBuilder(pathRoute);
			Route route1 = getRoute(eth.getDestinationMAC().toLong(), eth.getSourceMAC().toLong(), sb.reverse().toString());
			// Write the path into the flow tables of the switches on the path.
			if (route != null && route1 != null) {
				OFMatch match1 = new OFMatch();
				match1.setNetworkDestination(match.getNetworkSource());
				match1.setNetworkSource(match.getNetworkDestination());
				installRoute(route1.getPath(), match1);
				installRoute(route.getPath(), match);
			}
			
			
			return Command.STOP;
		}
	}

	public String calculateRoute(Long src, Long dst) {
		//System.out.println(src + " to " + dst);
		System.out.print("route: ");
		//here we want to use the algo to compute the route and then create the Route object
		//and send it back up for connection, we only need to tell the Route what NodePortTuples to use
		//and we can get this info from the topology and linkPaths data structures
		//we can use this to compute all of our costs starting from src
		Map<Long, Long> pathCost = new HashMap<>();
		long start = 0;
		pathCost.put(src, start);
		//set to make sure we do not recalculate a switch
		Set<Long> seen = new HashSet<>();
		Map<Long, String> path = new HashMap<>();
		path.put(src, "" + src);
		
		//we want to calculate for all switches in the topology
		for(int i = 0; i < topology.keySet().size(); i++)
		{
			//find the switch with the smallest cost available that has not been visited
			long min = findMin(seen, pathCost);
			
			//mark the switch as seen
			seen.add(min);
			
			//update all of the neighbors of min and set their new costs
			Iterator<Long> neis = topology.get(min).iterator();
			
			while(neis.hasNext())
			{
				long cur = neis.next();
				long cost = 0;
				
				//both even
				if(cur % 2 == 0 && min % 2 == 0) cost = 100;
				//both odd
				else if(cur % 2 == 1 && min % 2 == 1) cost = 1;
				//anything else
				else cost = 10;
				
				if(!pathCost.containsKey(cur) || (!seen.contains(cur) && pathCost.get(cur) > pathCost.get(min) + cost))
				{
					pathCost.put(cur, pathCost.get(min) + cost);
					path.put(cur, path.get(min) + " " + cur);
				}
			}
		}
		
		//now we should know the best path from src to dst
		System.out.println(path.get(dst));
		
		/* Print test to see what the smallest cost to each node is from the source
		for(Long swit : pathCost.keySet())
		{
			System.out.println("From: " + src + " to " + swit + " cost is " + pathCost.get(swit));
		}
		*/
		
		return path.get(dst);
	}
	
	public Route getRoute(long src, long dst, String path) {
		//now lets try to build the routes
		//we need one Route from src to dest
		//and another from dest to src
		//we can flip the String and src and dest to get the result
		
		Route routePath = new Route(src, dst);
		List<NodePortTuple> list = new ArrayList<>();
		
		String[] nodePath = path.split(" ");
		
		for(int i = 0; i < nodePath.length - 1; i++)
		{
			//out port
			String lookup = nodePath[i] + "-" + nodePath[i + 1];
			list.add(new NodePortTuple(Long.parseLong(nodePath[i]), portLinks.get(lookup)));
			//in port
			lookup = nodePath[i + 1] + "-" + nodePath[i];
			list.add(new NodePortTuple(Long.parseLong(nodePath[i + 1]), portLinks.get(lookup)));
		}
		
		routePath.setPath(list);
		
		/* Print to test Route path to see if they are correct outports and source MAC
		for(NodePortTuple node : list)
		{
			System.out.println(node);
		}
		*/
		
		return routePath;
	}
	
	public long findMin(Set<Long> seen, Map<Long, Long> map) {
		
		long res = Long.MAX_VALUE;
		long swi = -1;
		
		for(Long sw : map.keySet())
		{
			if(map.get(sw) < res && !seen.contains(sw))
			{
				res = map.get(sw);
				swi = sw;
			}
		}
		
		return swi; 
	}
	
	// Install routing rules on switches. 
	private void installRoute(List<NodePortTuple> path, OFMatch match) {

		OFMatch m = new OFMatch();

		m.setDataLayerType(Ethernet.TYPE_IPv4)
				.setNetworkSource(match.getNetworkSource())
				.setNetworkDestination(match.getNetworkDestination());

		for (int i = 0; i <= path.size() - 1; i += 2) {
			short inport = path.get(i).getPortId();
			m.setInputPort(inport);
			List<OFAction> actions = new ArrayList<OFAction>();
			OFActionOutput outport = new OFActionOutput(path.get(i + 1)
					.getPortId());
			actions.add(outport);

			OFFlowMod mod = (OFFlowMod) floodlightProvider
					.getOFMessageFactory().getMessage(OFType.FLOW_MOD);
			mod.setCommand(OFFlowMod.OFPFC_ADD)
					.setIdleTimeout((short) 0)
					.setHardTimeout((short) 0)
					.setMatch(m)
					.setPriority((short) 105)
					.setActions(actions)
					.setLength(
							(short) (OFFlowMod.MINIMUM_LENGTH + OFActionOutput.MINIMUM_LENGTH));
			flowPusher.addFlow("routeFlow" + uniqueFlow, mod,
					HexString.toHexString(path.get(i).getNodeId()));
			uniqueFlow++;
		}
	}
}
