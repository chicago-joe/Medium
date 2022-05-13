import numpy as np
import pandas as pd
import matplotlib. pyplot as plt
from matplotlib import colors
from matplotlib.colors import LinearSegmentedColormap

# Color Palette
srColors = ['rgba(93, 164, 214, .8)', # Light Blue
            'rgba(255, 144, 14, .5)', # Orange
            'rgba(44, 160, 101, .6)',  # Green
            'rgba(255, 65, 54, .8)', # Red
            'rgba(204,204,204,1)', # Grey
            '#9467bd'] # Purple

#### Color Map Functions ####

def display_cmap(cmap): 
    #Display  a colormap cmap
    plt.imshow(np.linspace(0, 100, 256)[None, :],  
                aspect=25, 
                interpolation='nearest', 
                cmap=cmap) 
    plt.axis('off')

def colormap_to_colorscale(cmap):
    from matplotlib import colors
    #function that transforms a matplotlib colormap to a Plotly colorscale
    plcs = [[k*0.1, colors.rgb2hex(cmap(k*0.1))] for k in range(11)]
    return plcs

def colorscale_from_list(alist, name, display=False): 
    # Defines a colormap, and the corresponding Plotly colorscale from the list alist
    # alist=the list of basic colors
    # name is the name of the corresponding matplotlib colormap
    
    cmap = LinearSegmentedColormap.from_list(name, alist)
    if display == True: display_cmap(cmap)
    colorscale=colormap_to_colorscale(cmap)
    return cmap, colorscale

def asymmetric_colorscale(data,  div_cmap, ref_point=0.0, step=0.05, fCmapOnly = False):
    #data: data can be a DataFrame, list of equal length lists, np.array, np.ma.array
    #div_cmap is the symmetric diverging matplotlib or custom colormap
    #ref_point:  reference point
    #step:  is step size for t in [0,1] to evaluate the colormap at t
   
    if isinstance(data, pd.DataFrame):
        D = data.values
    elif isinstance(data, np.ma.core.MaskedArray):
        D=np.ma.copy(data)
    else:    
        D=np.asarray(data, dtype=np.float) 
    
    dmin=np.nanmin(D)
    dmax=np.nanmax(D)
    if not (dmin < ref_point < dmax):
        raise ValueError('data are not appropriate for a diverging colormap')
        
    if dmax+dmin > 2.0*ref_point:
        left=2*ref_point-dmax
        right=dmax
        
        s=normalize(dmin, left,right)
        refp_norm=normalize(ref_point, left, right)# normalize reference point
        
        T=np.arange(refp_norm, s, -step).tolist()+[s]
        T=T[::-1]+np.arange(refp_norm+step, 1, step).tolist()
        
        
    else: 
        left=dmin
        right=2*ref_point-dmin
        
        s=normalize(dmax, left,right) 
        refp_norm=normalize(ref_point, left, right)
        
        T=np.arange(refp_norm, 0, -step).tolist()+[0]
        T=T[::-1]+np.arange(refp_norm+step, s, step).tolist()+[s]
        
    L=len(T)
    T_norm=[normalize(T[k],T[0],T[-1]) for k in range(L)] #normalize T values 
    csMap = [[T_norm[k], colors.rgb2hex(div_cmap(T[k]))] for k in range(L)]

    if fCmapOnly == True:
      return csMap
    else:
      return ApplyCmap(data, csMap)

def ApplyCmap(data, csMap, ApplyMinMax = []):
  a = []
  for i in normarr(data, ApplyMinMax):
      indx = np.abs(np.array(csMap)[:,0].astype(float) - i).argmin()
      a.append(csMap[indx][1])
  return a

def normalize(x,a,b): 
    if a>=b:
        raise ValueError('(a,b) is not an interval')
    return float(x-a)/(b-a)

def normarr(x, MinMax = []):
    x = np.array(x)
    if MinMax == []:
      a=np.min(x)
      b=np.max(x)
    else:
      a=MinMax[0]
      b=MinMax[1]
    if a>=b:
        raise ValueError('(a,b) is not an interval')
    return np.array((x-a)/(b-a))