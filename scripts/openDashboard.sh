#!/bin/bash
echo "Opening Roco Mission Control tunnel..."
echo "Dashboard will be at: http://localhost:3000"
echo "Press Ctrl+C to close the tunnel"
ssh -N -L 3000:localhost:3000 root@76.13.44.185
