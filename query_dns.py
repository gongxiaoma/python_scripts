import dns.resolver


def try_query(qname, rdtype):
    try:
        return list(dns.resolver.query(qname, rdtype))
    except dns.exception.DNSException:
        return []


def main(domain_list, f_mx, f_a):
    for domain in domain_list:
        l = try_query(domain, 'mx')
        if len(l) == 0:
            print 'query mx failed, domain={}'.format(domain)

        for d in l:
            mx_domain = str(d.exchange).strip('.')

            # save mx
            print >> f_mx, '{} {} {}'.format(domain, d.preference, mx_domain)

            m = try_query(mx_domain, 'a')
            if len(m) == 0:
                print 'query a failed, domain={}'.format(mx_domain)
            for e in m:
                # save a
                print >> f_a, '{} = {} for [9999] more minutes.'.format(mx_domain, str(e))


domain_list = open('e:/name.txt').read().split()
with open('e:/mx.txt', 'w') as f_mx, open('e:/a.txt', 'w') as f_a:
    main(domain_list, f_mx, f_a)
